package storage

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Output kinds. Both write to plain PostgreSQL; neither needs TimescaleDB.
const (
	KindPostgres       = "postgres"        // one column per register
	KindPostgresShadow = "postgres_shadow" // one JSONB payload per row
)

// legacyTypes maps the old type names, kept as aliases, to the current kind.
var legacyTypes = map[string]string{
	"timescaledb":        KindPostgres,
	"timescaledb_shadow": KindPostgresShadow,
}

type FileConfig struct {
	Storage Config `yaml:"storage"`
}

type Config struct {
	Outputs []OutputConfig `yaml:"outputs"`
}

type OutputConfig struct {
	Name           string         `yaml:"name"`
	Type           string         `yaml:"type"`
	Enabled        *bool          `yaml:"enabled,omitempty"`
	Postgres       PostgresConfig `yaml:"postgres,omitempty"`
	PostgresShadow PostgresConfig `yaml:"postgres_shadow,omitempty"`

	// Legacy blocks, read when the new one is empty.
	Timescaledb       PostgresConfig `yaml:"timescaledb,omitempty"`
	TimescaledbShadow PostgresConfig `yaml:"timescaledb_shadow,omitempty"`
}

// PostgresConfig is the connection and target table of a PostgreSQL output.
// Connection values are read from the named environment variables.
type PostgresConfig struct {
	HostEnv     string `yaml:"host_env"`
	PortEnv     string `yaml:"port_env"`
	UserEnv     string `yaml:"user_env"`
	PasswordEnv string `yaml:"password_env"`
	DatabaseEnv string `yaml:"database_env"`
	Schema      string `yaml:"schema"`
	Table       string `yaml:"table"`
}

func (c PostgresConfig) isZero() bool {
	return c == PostgresConfig{}
}

func (o OutputConfig) IsEnabled() bool {
	return o.Enabled == nil || *o.Enabled
}

// Resolve returns the output kind and its settings. Legacy type names and
// blocks are accepted; deprecated is non-empty when one was used, with a
// message meant for the run log.
func (o OutputConfig) Resolve() (kind string, cfg PostgresConfig, deprecated string, err error) {
	typ := strings.ToLower(strings.TrimSpace(o.Type))
	kind = typ
	if current, ok := legacyTypes[typ]; ok {
		kind = current
		deprecated = fmt.Sprintf("storage output %q uses type %q, renamed to %q", o.Name, typ, current)
	}

	switch kind {
	case KindPostgres:
		cfg = o.Postgres
		if cfg.isZero() && !o.Timescaledb.isZero() {
			cfg = o.Timescaledb
			deprecated = joinNotes(deprecated, fmt.Sprintf("storage output %q uses block \"timescaledb:\", renamed to \"postgres:\"", o.Name))
		}
	case KindPostgresShadow:
		cfg = o.PostgresShadow
		if cfg.isZero() && !o.TimescaledbShadow.isZero() {
			cfg = o.TimescaledbShadow
			deprecated = joinNotes(deprecated, fmt.Sprintf("storage output %q uses block \"timescaledb_shadow:\", renamed to \"postgres_shadow:\"", o.Name))
		}
	default:
		return "", PostgresConfig{}, "", fmt.Errorf("storage output %q: unsupported type %q (supported: %s, %s)", o.Name, o.Type, KindPostgres, KindPostgresShadow)
	}
	return kind, cfg, deprecated, nil
}

func joinNotes(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var wrapper FileConfig
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}
	if len(wrapper.Storage.Outputs) == 0 {
		return nil, fmt.Errorf("storage.outputs is empty")
	}
	for i, output := range wrapper.Storage.Outputs {
		if output.Name == "" {
			return nil, fmt.Errorf("storage.outputs[%d].name is empty", i)
		}
		if output.Type == "" {
			return nil, fmt.Errorf("storage.outputs[%d].type is empty", i)
		}
		// Reject an unknown type when loading, so --checkConfig catches it
		// before deploy instead of the agent failing on every run.
		if _, _, _, err := output.Resolve(); err != nil {
			return nil, fmt.Errorf("storage.outputs[%d]: %w", i, err)
		}
	}

	return &wrapper.Storage, nil
}
