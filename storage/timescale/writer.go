package timescale

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const backendType = "timescaledb"

var nonIdentChars = regexp.MustCompile(`[^a-z0-9_]+`)
var multiUnderscore = regexp.MustCompile(`_+`)

type Writer struct {
	name      string
	available bool

	schema string
	table  string
	fqn    string

	pool         *pgxpool.Pool
	knownColumns map[string]struct{}
	warnedIgnore map[string]struct{}
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
		name:         name,
		schema:       schema,
		table:        table,
		fqn:          schema + "." + table,
		pool:         pool,
		warnedIgnore: map[string]struct{}{},
	}

	if err := pool.Ping(context.Background()); err != nil {
		fmt.Printf("WARNING: storage output %q (type=%s) is not reachable: %v\n", name, backendType, err)
		w.available = false
	} else {
		w.available = true
		fmt.Printf("Storage output %q (type=%s) is reachable\n", name, backendType)
		if err := w.refreshKnownColumns(context.Background()); err != nil {
			fmt.Printf("WARNING: storage output %q (type=%s) could not load table columns for %s: %v\n", name, backendType, w.fqn, err)
		}
	}

	return w, nil
}

func (w *Writer) Name() string {
	if w == nil {
		return ""
	}
	return w.name
}

func (w *Writer) Write(tags map[string]string, fields map[string]interface{}, ts time.Time) {
	if w == nil || !w.available || w.pool == nil {
		return
	}

	deviceName := firstNonEmpty(tags["device_name"], tags["device"])
	slaveName := firstNonEmpty(tags["slave_name"], tags["slave"])
	slaveIDRaw := strings.TrimSpace(tags["slave_id"])
	if deviceName == "" || slaveName == "" || slaveIDRaw == "" {
		fmt.Printf("Warning writing to storage output %q (type=%s): missing required tags device_name/slave_name/slave_id\n", w.name, backendType)
		return
	}
	slaveID, err := strconv.Atoi(slaveIDRaw)
	if err != nil {
		fmt.Printf("Warning writing to storage output %q (type=%s): invalid slave_id=%q\n", w.name, backendType, slaveIDRaw)
		return
	}

	tagColumns := collectTagColumns(tags)
	fieldColumns := collectFieldColumns(fields)

	tagColumns = w.filterKnownColumns(tagColumns)
	fieldColumns = w.filterKnownColumns(fieldColumns)

	if len(fieldColumns) == 0 {
		return
	}

	columns := make([]string, 0, 4+len(tagColumns)+len(fieldColumns))
	args := make([]interface{}, 0, 4+len(tagColumns)+len(fieldColumns))

	columns = append(columns, "ts", "device_name", "slave_name", "slave_id")
	args = append(args, ts.UTC(), deviceName, slaveName, slaveID)

	baseUsed := map[string]int{
		"ts":          1,
		"device_name": 1,
		"slave_name":  1,
		"slave_id":    1,
	}

	tagColNames := make([]string, 0, len(tagColumns))
	for _, c := range tagColumns {
		name := makeUnique(c.Name, baseUsed)
		tagColNames = append(tagColNames, name)
		columns = append(columns, name)
		args = append(args, c.Value)
	}

	fieldColNames := make([]string, 0, len(fieldColumns))
	for _, c := range fieldColumns {
		name := makeUnique(c.Name, baseUsed)
		fieldColNames = append(fieldColNames, name)
		columns = append(columns, name)
		args = append(args, c.Value)
	}

	var placeholders []string
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	updateSet := make([]string, 0, 1+len(tagColNames)+len(fieldColNames))
	updateSet = append(updateSet, "slave_id = EXCLUDED.slave_id")
	for _, c := range tagColNames {
		updateSet = append(updateSet, c+" = EXCLUDED."+c)
	}
	for _, c := range fieldColNames {
		updateSet = append(updateSet, c+" = EXCLUDED."+c)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (ts, device_name, slave_name) DO UPDATE SET %s",
		w.fqn,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateSet, ", "),
	)

	if _, err := w.pool.Exec(context.Background(), query, args...); err != nil {
		if isUndefinedColumnErr(err) {
			// Column set likely changed; refresh cache and continue next samples.
			if refreshErr := w.refreshKnownColumns(context.Background()); refreshErr != nil {
				fmt.Printf("WARNING: storage output %q (type=%s) failed refreshing known columns after undefined column error: %v\n", w.name, backendType, refreshErr)
			}
		}
		fmt.Printf("Error writing to storage output %q (type=%s): %v\n", w.name, backendType, err)
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

func (w *Writer) refreshKnownColumns(ctx context.Context) error {
	if w == nil || w.pool == nil {
		return nil
	}
	rows, err := w.pool.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
	`, w.schema, w.table)
	if err != nil {
		return err
	}
	defer rows.Close()

	known := map[string]struct{}{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		known[strings.ToLower(strings.TrimSpace(col))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(known) == 0 {
		return fmt.Errorf("table %s has no visible columns", w.fqn)
	}
	w.knownColumns = known
	return nil
}

func (w *Writer) filterKnownColumns(in []namedValue) []namedValue {
	if len(in) == 0 {
		return in
	}
	// If we don't have a cached schema yet, keep current behavior (best effort).
	if len(w.knownColumns) == 0 {
		return in
	}

	out := make([]namedValue, 0, len(in))
	for _, c := range in {
		key := strings.ToLower(strings.TrimSpace(c.Name))
		if _, ok := w.knownColumns[key]; ok {
			out = append(out, c)
			continue
		}
		w.warnIgnoredColumn(c.Name)
	}
	return out
}

func (w *Writer) warnIgnoredColumn(columnName string) {
	if w == nil {
		return
	}
	key := strings.ToLower(strings.TrimSpace(columnName))
	if key == "" {
		return
	}
	if _, seen := w.warnedIgnore[key]; seen {
		return
	}
	w.warnedIgnore[key] = struct{}{}
	fmt.Printf("Warning writing to storage output %q (type=%s): ignoring unknown column %q in table %s\n", w.name, backendType, columnName, w.fqn)
}

type namedValue struct {
	Name  string
	Value interface{}
}

func collectTagColumns(tags map[string]string) []namedValue {
	if len(tags) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]namedValue, 0, len(keys))
	for _, k := range keys {
		v := strings.TrimSpace(tags[k])
		if v == "" {
			continue
		}
		if shouldSkipTagKey(k) {
			continue
		}
		name := normalizeTagColumnName(k)
		if name == "" {
			continue
		}
		out = append(out, namedValue{Name: name, Value: v})
	}
	return out
}

func collectFieldColumns(fields map[string]interface{}) []namedValue {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]namedValue, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "raw_") {
			continue
		}
		name := sanitizeIdentifier(k)
		if name == "" || name == "ts" || name == "device_name" || name == "slave_name" || name == "slave_id" {
			continue
		}
		out = append(out, namedValue{Name: name, Value: normalizeValue(fields[k])})
	}
	return out
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

func shouldSkipTagKey(tagKey string) bool {
	switch strings.ToLower(strings.TrimSpace(tagKey)) {
	case "ts", "device", "device_name", "slave", "slave_name", "slave_id", "unit":
		return true
	default:
		return false
	}
}

func normalizeTagColumnName(tagKey string) string {
	key := strings.ToLower(strings.TrimSpace(tagKey))
	switch key {
	case "ip":
		return "ip_address"
	default:
		return sanitizeIdentifier(key)
	}
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

func makeUnique(base string, used map[string]int) string {
	if _, ok := used[base]; !ok {
		used[base] = 1
		return base
	}
	n := used[base]
	for {
		candidate := fmt.Sprintf("%s_%d", base, n+1)
		if _, exists := used[candidate]; !exists {
			used[base] = n + 1
			used[candidate] = 1
			return candidate
		}
		n++
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
