package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/atrabilis/modbus-agent/internal"
)

// Every datatype the validation accepts must be one the decoder handles, so a
// type added to one place and not the other is caught here.
func TestSupportedDatatypesAreDecodable(t *testing.T) {
	for _, dt := range internal.SupportedDatatypes() {
		raw := make([]byte, 8)
		if _, ok := decode(t, dt, 1, raw); !ok {
			t.Fatalf("validation accepts %q but decodeRegisterValue rejects it", dt)
		}
	}
}

const validConfig = `
plant: plant_a
devices:
  - device:
      name: logger
      ip: 192.0.2.10
      port: 502
      slaves:
        - name: inv1
          slave_id: 1
          modbus_registers:
            - register: 5001
              name: nominal_active_power
              words: 1
              datatype: U16
              gain: 0.1
            - register: 5093
              name: daily_export_energy
              words: 2
              datatype: U32LE
              gain: 0.1
storage:
  outputs:
    - name: local_shadow
      type: postgres_shadow
      postgres_shadow:
        host_env: PG_HOST
        port_env: PG_PORT
        user_env: PG_USER
        password_env: PG_PASSWORD
        database_env: PG_DB
        schema: edge
        table: logger_shadow
`

const invalidConfig = `
plant: plant_a
devices:
  - device:
      name: smartlogger
      ip: 192.0.2.11
      port: 502
      slaves:
        - name: logger
          slave_id: 0
          modbus_registers:
            - register: 40550
              name: co2_reduction_extended
              words: 4
              datatype: U64
              gain: 0.01
            - register: 40560
              name: total_power
              words: 1
              datatype: S32
            - register: 40570
              name: frequency
              words: 1
              datatype: U16
storage:
  outputs:
    - name: local_shadow
      type: postgres_shadow
      postgres_shadow:
        host_env: PG_HOST
        port_env: PG_PORT
        user_env: PG_USER
        password_env: PG_PASSWORD
        database_env: PG_DB
        schema: edge
        table: logger_shadow
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCheckConfigValid(t *testing.T) {
	var out strings.Builder
	if code := validateConfigFile(writeConfig(t, validConfig), &out); code != 0 {
		t.Fatalf("exit %d, output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "2 registers OK") {
		t.Fatalf("output: %q", out.String())
	}
}

func TestCheckConfigInvalid(t *testing.T) {
	var out strings.Builder
	if code := validateConfigFile(writeConfig(t, invalidConfig), &out); code != 1 {
		t.Fatalf("exit %d, want 1; output:\n%s", code, out.String())
	}
	got := out.String()
	for _, want := range []string{
		`INVALID device="smartlogger" slave="logger" register=40550 name="co2_reduction_extended": unknown datatype "U64"`,
		`register=40560 name="total_power": words=1: datatype "S32" needs at least 2`,
		"2 of 3 registers are invalid",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "frequency") {
		t.Fatalf("valid register reported as invalid:\n%s", got)
	}
}

func TestCheckConfigUnreadableFile(t *testing.T) {
	var out strings.Builder
	if code := validateConfigFile(filepath.Join(t.TempDir(), "missing.yml"), &out); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(out.String(), "cannot load registers file") {
		t.Fatalf("output: %q", out.String())
	}
}

func TestCheckConfigStorageAndResolution(t *testing.T) {
	cases := []struct {
		name     string
		config   string
		wantCode int
		want     string
	}{
		{
			name:     "legacy type name only warns",
			config:   strings.Replace(strings.Replace(validConfig, "type: postgres_shadow", "type: timescaledb_shadow", 1), "postgres_shadow:", "timescaledb_shadow:", 1),
			wantCode: 0,
			want:     `WARNING: storage output "local_shadow" uses type "timescaledb_shadow", renamed to "postgres_shadow"`,
		},
		{
			name:     "removed influxdb2 output",
			config:   strings.Replace(validConfig, "type: postgres_shadow", "type: influxdb2", 1),
			wantCode: 1,
			want:     `unsupported type "influxdb2"`,
		},
		{
			name:     "missing connection field",
			config:   strings.Replace(validConfig, "        table: logger_shadow\n", "", 1),
			wantCode: 1,
			want:     "postgres_shadow.table is empty",
		},
		{
			name:     "no storage section",
			config:   validConfig[:strings.Index(validConfig, "storage:")],
			wantCode: 1,
			want:     "INVALID storage: storage.outputs is empty",
		},
		{
			name:     "valid timestamp resolution",
			config:   strings.Replace(validConfig, "plant: plant_a", "plant: plant_a\ntimestamp_resolution: 10s", 1),
			wantCode: 0,
			want:     "2 registers OK",
		},
		{
			name:     "invalid timestamp resolution",
			config:   strings.Replace(validConfig, "plant: plant_a", "plant: plant_a\ntimestamp_resolution: 1 minute", 1),
			wantCode: 1,
			want:     `INVALID timestamp_resolution "1 minute"`,
		},
		{
			name:     "negative timestamp resolution",
			config:   strings.Replace(validConfig, "plant: plant_a", "plant: plant_a\ntimestamp_resolution: -1m", 1),
			wantCode: 1,
			want:     "must not be negative",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out strings.Builder
			if code := validateConfigFile(writeConfig(t, tc.config), &out); code != tc.wantCode {
				t.Fatalf("exit %d, want %d; output:\n%s", code, tc.wantCode, out.String())
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("output missing %q:\n%s", tc.want, out.String())
			}
		})
	}
}
