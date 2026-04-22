package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigParsesTimescaledbShadowOutput(t *testing.T) {
	t.Parallel()

	yaml := `storage:
  outputs:
    - name: "shadow_local"
      type: "timescaledb_shadow"
      enabled: true
      timescaledb_shadow:
        host_env: "TS_HOST"
        port_env: "TS_PORT"
        user_env: "TS_USER"
        password_env: "TS_PASSWORD"
        database_env: "TS_DB"
        schema: "landing"
        table: "diris_i35_shadow"
`

	dir := t.TempDir()
	path := filepath.Join(dir, "storage.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if len(cfg.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(cfg.Outputs))
	}

	out := cfg.Outputs[0]
	if out.Type != "timescaledb_shadow" {
		t.Fatalf("expected type timescaledb_shadow, got %q", out.Type)
	}
	if out.TimescaledbShadow.Schema != "landing" {
		t.Fatalf("expected schema landing, got %q", out.TimescaledbShadow.Schema)
	}
	if out.TimescaledbShadow.Table != "diris_i35_shadow" {
		t.Fatalf("expected table diris_i35_shadow, got %q", out.TimescaledbShadow.Table)
	}
}
