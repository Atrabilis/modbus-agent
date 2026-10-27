package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadFromString(t *testing.T, yaml string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return LoadConfig(path)
}

const connBlock = `
        host_env: "PG_HOST"
        port_env: "PG_PORT"
        user_env: "PG_USER"
        password_env: "PG_PASSWORD"
        database_env: "PG_DB"
        schema: "edge"
        table: "diris_i35_shadow"
`

func TestLoadConfigResolvesPostgresShadow(t *testing.T) {
	t.Parallel()

	cfg, err := loadFromString(t, `storage:
  outputs:
    - name: "shadow_local"
      type: "postgres_shadow"
      postgres_shadow:`+connBlock)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	kind, pg, deprecated, err := cfg.Outputs[0].Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if kind != KindPostgresShadow || pg.Schema != "edge" || pg.Table != "diris_i35_shadow" || pg.HostEnv != "PG_HOST" {
		t.Fatalf("unexpected resolution: kind=%q cfg=%+v", kind, pg)
	}
	if deprecated != "" {
		t.Fatalf("current names must not warn, got %q", deprecated)
	}
}

func TestLoadConfigAcceptsLegacyNamesWithWarning(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, yaml, wantKind string
	}{
		{"legacy shadow type and block", `storage:
  outputs:
    - name: "shadow_local"
      type: "timescaledb_shadow"
      timescaledb_shadow:` + connBlock, KindPostgresShadow},
		{"legacy typed type and block", `storage:
  outputs:
    - name: "typed"
      type: "timescaledb"
      timescaledb:` + connBlock, KindPostgres},
		{"new type with legacy block", `storage:
  outputs:
    - name: "shadow_local"
      type: "postgres_shadow"
      timescaledb_shadow:` + connBlock, KindPostgresShadow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadFromString(t, tc.yaml)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			kind, pg, deprecated, err := cfg.Outputs[0].Resolve()
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if kind != tc.wantKind || pg.Table != "diris_i35_shadow" {
				t.Fatalf("kind=%q table=%q, want %q and the legacy block's table", kind, pg.Table, tc.wantKind)
			}
			if deprecated == "" {
				t.Fatal("a legacy name must produce a deprecation note")
			}
		})
	}
}

func TestLoadConfigRejectsUnknownType(t *testing.T) {
	t.Parallel()

	for _, typ := range []string{"influxdb2", "timescale", "postgresql"} {
		_, err := loadFromString(t, `storage:
  outputs:
    - name: "remote"
      type: "`+typ+`"
`)
		if err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Fatalf("type %q: expected an unsupported type error, got %v", typ, err)
		}
	}
}
