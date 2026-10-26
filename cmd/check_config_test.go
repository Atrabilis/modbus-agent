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
