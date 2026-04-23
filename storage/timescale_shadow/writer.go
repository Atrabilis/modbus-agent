package timescale_shadow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	agentinternal "github.com/atrabilis/modbus-agent/internal"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const backendType = "timescaledb_shadow"

var nonIdentChars = regexp.MustCompile(`[^a-z0-9_]+`)
var multiUnderscore = regexp.MustCompile(`_+`)

type Writer struct {
	name      string
	available bool

	schema string
	table  string
	fqn    string

	pool *pgxpool.Pool

	conflictMu          sync.RWMutex
	conflictOnSeriesKey bool
}

func NewWriter(name string, cfg Config) (*Writer, error) {
	if err := cfg.Validate(name); err != nil {
		return nil, err
	}

	host := strings.TrimSpace(os.Getenv(cfg.HostEnv))
	port := strings.TrimSpace(os.Getenv(cfg.PortEnv))
	user := strings.TrimSpace(os.Getenv(cfg.UserEnv))
	password := os.Getenv(cfg.PasswordEnv)
	database := strings.TrimSpace(os.Getenv(cfg.DatabaseEnv))
	if host == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.HostEnv)
	}
	if port == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.PortEnv)
	}
	if user == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.UserEnv)
	}
	if password == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.PasswordEnv)
	}
	if database == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.DatabaseEnv)
	}

	schema := sanitizeIdentifier(cfg.Schema)
	table := sanitizeIdentifier(cfg.Table)
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host,
		port,
		url.QueryEscape(database),
	)

	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool for output %q: %w", name, err)
	}

	w := &Writer{
		name:                name,
		schema:              schema,
		table:               table,
		fqn:                 schema + "." + table,
		pool:                pool,
		conflictOnSeriesKey: true,
	}

	if err := pool.Ping(context.Background()); err != nil {
		fmt.Printf("WARNING: storage output %q (type=%s) is not reachable: %v\n", name, backendType, err)
		w.available = false
	} else {
		w.available = true
		fmt.Printf("Storage output %q (type=%s) is reachable\n", name, backendType)
	}

	return w, nil
}

func (w *Writer) Name() string {
	if w == nil {
		return ""
	}
	return w.name
}

func (w *Writer) Available() bool {
	if w == nil {
		return false
	}
	return w.available
}

func (w *Writer) Write(tags map[string]string, fields map[string]interface{}, ts time.Time) {
	if w == nil || !w.available || w.pool == nil {
		return
	}

	plant := strings.TrimSpace(tags["plant"])
	deviceName := firstNonEmpty(tags["device_name"], tags["device"])
	slaveName := firstNonEmpty(tags["slave_name"], tags["slave"])
	slaveIDRaw := strings.TrimSpace(tags["slave_id"])
	if plant == "" || deviceName == "" || slaveName == "" || slaveIDRaw == "" {
		fmt.Printf("Warning writing to storage output %q (type=%s): missing required tags plant/device_name/slave_name/slave_id\n", w.name, backendType)
		return
	}

	slaveID, err := strconv.Atoi(slaveIDRaw)
	if err != nil {
		fmt.Printf("Warning writing to storage output %q (type=%s): invalid slave_id=%q\n", w.name, backendType, slaveIDRaw)
		return
	}

	payloadFields := collectPayloadFields(fields)
	if len(payloadFields) == 0 {
		return
	}

	seriesKey, flagsMap := agentinternal.BuildSeriesMetadata(tags)
	payload := map[string]interface{}{
		"slave_id":   slaveID,
		"series_key": seriesKey,
		"flags":      flagsMap,
		"fields":     payloadFields,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Warning writing to storage output %q (type=%s): could not encode payload as JSON: %v\n", w.name, backendType, err)
		return
	}

	useSeriesConflict := w.currentConflictOnSeriesKey()
	err = w.execUpsert(context.Background(), plant, ts.UTC(), deviceName, slaveName, json.RawMessage(payloadJSON), useSeriesConflict)
	if err == nil {
		return
	}

	if !isRetryableConflictTargetErr(err) {
		fmt.Printf("Error writing to storage output %q (type=%s): %v\n", w.name, backendType, err)
		return
	}

	alternateConflict := !useSeriesConflict
	alternateErr := w.execUpsert(context.Background(), plant, ts.UTC(), deviceName, slaveName, json.RawMessage(payloadJSON), alternateConflict)
	if alternateErr != nil {
		fmt.Printf("Error writing to storage output %q (type=%s): %v (fallback: %v)\n", w.name, backendType, err, alternateErr)
		return
	}

	if w.setConflictOnSeriesKey(alternateConflict) {
		switch {
		case alternateConflict:
			fmt.Printf(
				"Warning writing to storage output %q (type=%s): switched ON CONFLICT target to include series_key\n",
				w.name,
				backendType,
			)
		default:
			fmt.Printf(
				"Warning writing to storage output %q (type=%s): switched ON CONFLICT target to legacy key without series_key\n",
				w.name,
				backendType,
			)
		}
	}
}

func (w *Writer) Flush() {
	// No buffered writer for pgxpool; writes are synchronous in Write().
}

func (w *Writer) Close() {
	if w == nil {
		return
	}
	if w.pool != nil {
		w.pool.Close()
	}
}

func collectPayloadFields(fields map[string]interface{}) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := map[string]interface{}{}
	for _, k := range keys {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "raw_") {
			continue
		}
		name := sanitizeIdentifier(k)
		if name == "" || shouldSkipPayloadField(name) {
			continue
		}
		out[name] = normalizeValue(fields[k])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func shouldSkipPayloadField(name string) bool {
	switch name {
	case "ts", "plant", "device_name", "slave_name", "slave_id", "series_key", "flags", "payload", "ingested_at":
		return true
	default:
		return false
	}
}

func normalizeValue(v interface{}) interface{} {
	switch x := v.(type) {
	case float64:
		if isWholeNumber(x) && x >= math.MinInt64 && x <= math.MaxInt64 {
			return int64(x)
		}
		return x
	default:
		return v
	}
}

func isWholeNumber(v float64) bool {
	return math.Abs(v-math.Round(v)) < 1e-9
}

func sanitizeIdentifier(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	v = strings.ReplaceAll(v, "-", "_")
	v = strings.ReplaceAll(v, " ", "_")
	v = nonIdentChars.ReplaceAllString(v, "_")
	v = multiUnderscore.ReplaceAllString(v, "_")
	v = strings.Trim(v, "_")
	if v == "" {
		v = "field"
	}
	if v[0] >= '0' && v[0] <= '9' {
		v = "f_" + v
	}
	if isReservedWord(v) {
		v = v + "_col"
	}
	return v
}

func isReservedWord(v string) bool {
	switch v {
	case "all", "analyse", "analyze", "and", "any", "array", "as", "asc",
		"asymmetric", "authorization", "between", "binary", "both", "case",
		"cast", "check", "collate", "column", "constraint", "create", "current_catalog",
		"current_date", "current_role", "current_time", "current_timestamp",
		"current_user", "default", "deferrable", "desc", "distinct", "do", "else",
		"end", "except", "false", "for", "foreign", "from", "grant", "group",
		"having", "in", "initially", "intersect", "into", "leading", "limit",
		"localtime", "localtimestamp", "new", "not", "null", "off", "offset",
		"old", "on", "only", "or", "order", "placing", "primary", "references",
		"returning", "select", "session_user", "some", "symmetric", "table",
		"then", "to", "trailing", "true", "union", "unique", "user", "using",
		"variadic", "when", "where", "window", "with":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (w *Writer) currentConflictOnSeriesKey() bool {
	if w == nil {
		return true
	}
	w.conflictMu.RLock()
	defer w.conflictMu.RUnlock()
	return w.conflictOnSeriesKey
}

func (w *Writer) setConflictOnSeriesKey(v bool) bool {
	if w == nil {
		return false
	}
	w.conflictMu.Lock()
	defer w.conflictMu.Unlock()
	if w.conflictOnSeriesKey == v {
		return false
	}
	w.conflictOnSeriesKey = v
	return true
}

func (w *Writer) execUpsert(
	ctx context.Context,
	plant string,
	ts time.Time,
	deviceName string,
	slaveName string,
	payload json.RawMessage,
	conflictOnSeriesKey bool,
) error {
	if w == nil || w.pool == nil {
		return nil
	}
	conflictCols := "plant, device_name, slave_name, ts"
	if conflictOnSeriesKey {
		conflictCols += ", series_key"
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (plant, ts, device_name, slave_name, payload) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (%s) DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()",
		w.fqn,
		conflictCols,
	)
	_, err := w.pool.Exec(ctx, query, plant, ts, deviceName, slaveName, payload)
	return err
}

func isRetryableConflictTargetErr(err error) bool {
	return isNoMatchingConflictConstraintErr(err) || isUndefinedColumnErr(err)
}

func isNoMatchingConflictConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42P10"
	}
	return false
}

func isUndefinedColumnErr(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42703"
	}
	return false
}
