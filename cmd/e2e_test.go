package main

// End-to-end test: builds the real agent binary, runs it the way the systemd
// timer does (config file + dotenv file), against the fake Modbus server from
// poll_test.go and a real PostgreSQL, then checks the rows in the shadow table.
//
// It needs a PostgreSQL server and is skipped unless MODBUS_AGENT_E2E_PG is set
// to a connection URL, e.g. postgres://postgres:postgres@127.0.0.1:5432/postgres
// (CI provides one as a service container). If MODBUS_AGENT_E2E_COVERDIR is set,
// the binary is built with -cover and writes its coverage data there.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Same table layout the ansible-infra playbook ensure_timescale_shadow_table.yml creates.
const shadowTableDDL = `CREATE TABLE %s (
  plant TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  device_name TEXT NOT NULL,
  slave_name TEXT NOT NULL,
  payload JSONB NOT NULL,
  series_key TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (plant, device_name, slave_name, ts, series_key)
)`

func buildAgent(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "modbus-agent")
	args := []string{"build", "-o", bin}
	if os.Getenv("MODBUS_AGENT_E2E_COVERDIR") != "" {
		args = append(args, "-cover", "-coverpkg=github.com/atrabilis/modbus-agent/...")
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func runAgent(t *testing.T, bin, configPath, envPath string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-configPath", configPath, "-envPath", envPath)
	cmd.Env = os.Environ()
	if dir := os.Getenv("MODBUS_AGENT_E2E_COVERDIR"); dir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("agent run failed: %v\n%s", err, out)
	}
	return string(out)
}

func TestAgentBinaryWritesShadowRows(t *testing.T) {
	dsn := os.Getenv("MODBUS_AGENT_E2E_PG")
	if dsn == "" {
		t.Skip("MODBUS_AGENT_E2E_PG not set; skipping end-to-end test")
	}
	pgURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse MODBUS_AGENT_E2E_PG: %v", err)
	}
	password, _ := pgURL.User.Password()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	schema := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	table := schema + ".inverter_shadow"
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	if _, err := conn.Exec(ctx, fmt.Sprintf(shadowTableDDL, table)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dev := newFakeDevice()
	seedTestDevice(dev)
	dev.setHolding(1, 130, 0x7FC0, 0x0000) // quiet NaN as F32BE
	port := startModbusServer(t, dev)

	dir := t.TempDir()
	configTemplate := `plant: e2e_plant
timestamp_resolution: RESOLUTION
devices:
  - device:
      name: inverter_block
      ip: 127.0.0.1
      port: %d
      timeout_ms: 1000
      slaves:
        - name: inv1
          slave_id: 1
          offset: 0
          modbus_registers:
            - {register: 100, function_code: 3, name: ac_voltage, words: 1, datatype: U16, gain: 0.1}
            - {register: 101, function_code: 3, name: temperature, words: 1, datatype: S16, gain: 1}
            - {register: 102, function_code: 3, name: energy, words: 2, datatype: U32, gain: 1}
            - {register: 104, function_code: 3, name: frequency, words: 2, datatype: F32BE, gain: 1}
            - {register: 106, function_code: 3, name: total_wh, words: 4, datatype: U64BE, gain: 1}
            - {register: 120, function_code: 3, name: model, words: 2, datatype: STR, gain: 1}
            - {register: 130, function_code: 3, name: dc_power, words: 2, datatype: F32BE, gain: 1}
            - {register: 200, function_code: 4, name: status, words: 1, datatype: U16, gain: 1}
            - {register: 0, function_code: 1, name: relays, words: 8, datatype: U8, gain: 1}
            - {register: 300, function_code: 3, name: typo_register, words: 1, datatype: U61, gain: 1}
        - name: inv2
          slave_id: 2
          offset: 1
          modbus_registers:
            - {register: 4951, function_code: 3, name: ac_voltage, words: 1, datatype: U16, gain: 1}
storage:
  outputs:
    - name: local_shadow
      type: OUTPUT_TYPE
      enabled: true
      OUTPUT_TYPE:
        host_env: E2E_PG_HOST
        port_env: E2E_PG_PORT
        user_env: E2E_PG_USER
        password_env: E2E_PG_PASSWORD
        database_env: E2E_PG_DB
        schema: %s
        table: inverter_shadow
`
	writeAgentConfig := func(outputType, resolution string) string {
		config := strings.ReplaceAll(configTemplate, "OUTPUT_TYPE", outputType)
		config = fmt.Sprintf(strings.Replace(config, "RESOLUTION", resolution, 1), port, schema)
		path := filepath.Join(dir, outputType+"-"+resolution+".yml")
		if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	configPath := writeAgentConfig("postgres_shadow", "10s")
	env := fmt.Sprintf("E2E_PG_HOST=%s\nE2E_PG_PORT=%s\nE2E_PG_USER=%s\nE2E_PG_PASSWORD=%s\nE2E_PG_DB=%s\n",
		pgURL.Hostname(), pgURL.Port(), pgURL.User.Username(), password, strings.TrimPrefix(pgURL.Path, "/"))
	envPath := filepath.Join(dir, "agent.env")
	if err := os.WriteFile(envPath, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}

	bin := buildAgent(t)
	out := runAgent(t, bin, configPath, envPath)
	if !strings.Contains(out, `WARNING: skipping invalid register`) || !strings.Contains(out, "typo_register") {
		t.Fatalf("expected the invalid register to be reported and skipped\n%s", out)
	}
	if strings.Contains(out, "renamed to") {
		t.Fatalf("current output names must not produce a deprecation warning\n%s", out)
	}

	var tsMicros int64
	if err := conn.QueryRow(ctx, "SELECT (extract(epoch FROM ts) * 1000000)::bigint FROM "+table+" LIMIT 1").Scan(&tsMicros); err != nil {
		t.Fatalf("read ts: %v", err)
	}
	if tsMicros%10_000_000 != 0 {
		t.Fatalf("timestamp_resolution 10s: stored ts is not a multiple of 10 s (%d us)", tsMicros)
	}

	type row struct {
		slave   string
		slaveID float64
		fields  map[string]interface{}
	}
	readRows := func() []row {
		rows, err := conn.Query(ctx, "SELECT slave_name, payload FROM "+table+" WHERE plant = 'e2e_plant' AND device_name = 'inverter_block' ORDER BY ts, slave_name")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var got []row
		for rows.Next() {
			var slave string
			var payload []byte
			if err := rows.Scan(&slave, &payload); err != nil {
				t.Fatalf("scan: %v", err)
			}
			var p struct {
				SlaveID float64                `json:"slave_id"`
				Fields  map[string]interface{} `json:"fields"`
			}
			if err := json.Unmarshal(payload, &p); err != nil {
				t.Fatalf("payload %s: %v", payload, err)
			}
			got = append(got, row{slave: slave, slaveID: p.SlaveID, fields: p.Fields})
		}
		return got
	}

	rows := readRows()
	if len(rows) != 2 {
		t.Fatalf("expected one row per slave (2), got %d\n%s", len(rows), out)
	}
	inv1, inv2 := rows[0], rows[1]
	if inv1.slave != "inv1" || inv1.slaveID != 1 || inv2.slave != "inv2" || inv2.slaveID != 2 {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	wantNumbers := map[string]float64{
		"ac_voltage": 234.5, "temperature": -200, "energy": 70000, "frequency": 50.25,
		"total_wh": 1<<40 + 5, "status": 7, "relays": 5,
	}
	for name, want := range wantNumbers {
		got, ok := inv1.fields[name].(float64)
		if !ok || math.Abs(got-want) > 1e-9 {
			t.Errorf("inv1.%s = %v, want %v", name, inv1.fields[name], want)
		}
	}
	if inv1.fields["model"] != "AB12" {
		t.Errorf("inv1.model = %v, want AB12", inv1.fields["model"])
	}
	// NaN cannot travel as a JSON number; it must arrive as the string
	// PostgreSQL accepts for double precision, without losing the row.
	if inv1.fields["dc_power"] != "NaN" {
		t.Errorf("inv1.dc_power = %v, want \"NaN\"", inv1.fields["dc_power"])
	}
	if _, ok := inv1.fields["typo_register"]; ok {
		t.Error("an invalid register must not be written")
	}
	if got, _ := inv2.fields["ac_voltage"].(float64); got != 42 {
		t.Errorf("inv2.ac_voltage = %v, want 42", inv2.fields["ac_voltage"])
	}

	var nanAsDouble float64
	if err := conn.QueryRow(ctx, "SELECT (payload->'fields'->>'dc_power')::double precision FROM "+table+" WHERE slave_name = 'inv1' LIMIT 1").Scan(&nanAsDouble); err != nil {
		t.Fatalf("cast NaN payload to double precision: %v", err)
	}
	if !math.IsNaN(nanAsDouble) {
		t.Fatalf("expected PostgreSQL to read the payload as NaN, got %v", nanAsDouble)
	}

	// A second run, with the legacy output names, still writes. In the same
	// 10 s step it updates the rows instead of duplicating them; otherwise it
	// adds one row per slave for the new ts.
	legacyOut := runAgent(t, bin, writeAgentConfig("timescaledb_shadow", "10s"), envPath)
	if !strings.Contains(legacyOut, `uses type "timescaledb_shadow", renamed to "postgres_shadow"`) {
		t.Fatalf("legacy output names must be accepted with a warning\n%s", legacyOut)
	}
	var perTs []int
	tsRows, err := conn.Query(ctx, "SELECT count(*) FROM "+table+" GROUP BY ts ORDER BY ts")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for tsRows.Next() {
		var n int
		if err := tsRows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		perTs = append(perTs, n)
	}
	tsRows.Close()
	for _, n := range perTs {
		if n != 2 {
			t.Fatalf("expected 2 rows per timestamp after a second run, got %v", perTs)
		}
	}

	// timestamp_resolution 0s keeps the exact read time. With any rounding
	// the stored ts would be a whole number of seconds.
	runAgent(t, bin, writeAgentConfig("postgres_shadow", "0s"), envPath)
	var fractional int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE extract(microseconds FROM ts)::bigint % 1000000 <> 0").Scan(&fractional); err != nil {
		t.Fatalf("query: %v", err)
	}
	if fractional != 2 {
		t.Fatalf("timestamp_resolution 0s: expected 2 rows with sub-second ts, got %d", fractional)
	}
}

// The `postgres` output (one column per register) against a typed test table
// shaped like sql/postgres_example.sql. The table has no column for
// "frequency": that register must be skipped with a warning while the rest of
// the row is written.
func TestAgentBinaryWritesTypedRows(t *testing.T) {
	dsn := os.Getenv("MODBUS_AGENT_E2E_PG")
	if dsn == "" {
		t.Skip("MODBUS_AGENT_E2E_PG not set; skipping end-to-end test")
	}
	pgURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse MODBUS_AGENT_E2E_PG: %v", err)
	}
	password, _ := pgURL.User.Password()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	schema := fmt.Sprintf("e2e_typed_%d", time.Now().UnixNano())
	table := schema + ".inverter"
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	if _, err := conn.Exec(ctx, `CREATE TABLE `+table+` (
		ts TIMESTAMPTZ NOT NULL, device_name TEXT NOT NULL, slave_name TEXT NOT NULL, slave_id INTEGER NOT NULL,
		series_key TEXT NOT NULL, flags JSONB,
		ac_voltage DOUBLE PRECISION, temperature DOUBLE PRECISION, energy DOUBLE PRECISION,
		total_wh DOUBLE PRECISION, model TEXT, dc_power DOUBLE PRECISION, status DOUBLE PRECISION,
		PRIMARY KEY (ts, device_name, slave_name, series_key))`); err != nil {
		t.Fatal(err)
	}

	dev := newFakeDevice()
	seedTestDevice(dev)
	dev.setHolding(1, 130, 0x7FC0, 0x0000) // quiet NaN as F32BE
	port := startModbusServer(t, dev)

	dir := t.TempDir()
	writeConfig := func(outputType string) string {
		cfg := fmt.Sprintf(`plant: e2e_plant
timestamp_resolution: 10s
devices:
  - device:
      name: inverter_block
      ip: 127.0.0.1
      port: %d
      timeout_ms: 1000
      slaves:
        - name: inv1
          slave_id: 1
          modbus_registers:
            - {register: 100, function_code: 3, name: ac_voltage, words: 1, datatype: U16, gain: 0.1}
            - {register: 101, function_code: 3, name: temperature, words: 1, datatype: S16, gain: 1}
            - {register: 102, function_code: 3, name: energy, words: 2, datatype: U32, gain: 1}
            - {register: 104, function_code: 3, name: frequency, words: 2, datatype: F32BE, gain: 1}
            - {register: 106, function_code: 3, name: total_wh, words: 4, datatype: U64BE, gain: 1}
            - {register: 120, function_code: 3, name: model, words: 2, datatype: STR, gain: 1}
            - {register: 130, function_code: 3, name: dc_power, words: 2, datatype: F32BE, gain: 1}
            - {register: 200, function_code: 4, name: status, words: 1, datatype: U16, gain: 1}
        - name: inv2
          slave_id: 2
          offset: 1
          modbus_registers:
            - {register: 4951, function_code: 3, name: ac_voltage, words: 1, datatype: U16, gain: 1}
storage:
  outputs:
    - name: typed
      type: %s
      %s:
        host_env: E2E_PG_HOST
        port_env: E2E_PG_PORT
        user_env: E2E_PG_USER
        password_env: E2E_PG_PASSWORD
        database_env: E2E_PG_DB
        schema: %s
        table: inverter
`, port, outputType, outputType, schema)
		path := filepath.Join(dir, outputType+".yml")
		if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	env := fmt.Sprintf("E2E_PG_HOST=%s\nE2E_PG_PORT=%s\nE2E_PG_USER=%s\nE2E_PG_PASSWORD=%s\nE2E_PG_DB=%s\n",
		pgURL.Hostname(), pgURL.Port(), pgURL.User.Username(), password, strings.TrimPrefix(pgURL.Path, "/"))
	envPath := filepath.Join(dir, "agent.env")
	if err := os.WriteFile(envPath, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}

	bin := buildAgent(t)
	out := runAgent(t, bin, writeConfig("postgres"), envPath)
	if !strings.Contains(out, `ignoring unknown column "frequency"`) {
		t.Fatalf("a register without a column must be reported\n%s", out)
	}

	var acVoltage, temperature, energy, totalWh, dcPower, status float64
	var model, plant, seriesKey string
	var slaveID int
	if err := conn.QueryRow(ctx, `SELECT slave_id, ac_voltage, temperature, energy, total_wh, model, dc_power, status,
		flags->>'plant', series_key FROM `+table+` WHERE slave_name = 'inv1'`).Scan(
		&slaveID, &acVoltage, &temperature, &energy, &totalWh, &model, &dcPower, &status, &plant, &seriesKey); err != nil {
		t.Fatalf("read inv1 row: %v\n%s", err, out)
	}
	if slaveID != 1 || acVoltage != 234.5 || temperature != -200 || energy != 70000 || totalWh != float64(1<<40+5) ||
		model != "AB12" || status != 7 {
		t.Fatalf("unexpected inv1 row: slave_id=%d ac_voltage=%v temperature=%v energy=%v total_wh=%v model=%q status=%v",
			slaveID, acVoltage, temperature, energy, totalWh, model, status)
	}
	if !math.IsNaN(dcPower) {
		t.Fatalf("NaN must be stored as a float NaN, got %v", dcPower)
	}
	if plant != "e2e_plant" || seriesKey == "" {
		t.Fatalf("tags must go to flags and series_key: plant=%q series_key=%q", plant, seriesKey)
	}
	var inv2 float64
	if err := conn.QueryRow(ctx, "SELECT ac_voltage FROM "+table+" WHERE slave_name = 'inv2'").Scan(&inv2); err != nil || inv2 != 42 {
		t.Fatalf("inv2 ac_voltage = %v (%v), want 42", inv2, err)
	}

	// The legacy type name still works, and a run in the same 10 s step
	// updates the rows instead of duplicating them.
	legacyOut := runAgent(t, bin, writeConfig("timescaledb"), envPath)
	if !strings.Contains(legacyOut, `uses type "timescaledb", renamed to "postgres"`) {
		t.Fatalf("legacy output name must be accepted with a warning\n%s", legacyOut)
	}
	rows, err := conn.Query(ctx, "SELECT count(*) FROM "+table+" GROUP BY ts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("expected 2 rows (one per slave) per timestamp, got %d", n)
		}
	}
}
