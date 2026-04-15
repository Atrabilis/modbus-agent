package storage

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type FileConfig struct {
	Storage Config `yaml:"storage"`
}

type Config struct {
	Outputs []OutputConfig `yaml:"outputs"`
}

type OutputConfig struct {
	Name        string            `yaml:"name"`
	Type        string            `yaml:"type"`
	Enabled     *bool             `yaml:"enabled,omitempty"`
	Influxdb2   Influxdb2Config   `yaml:"influxdb2,omitempty"`
	Timescaledb TimescaledbConfig `yaml:"timescaledb,omitempty"`
}

type Influxdb2Config struct {
	HostEnv     string `yaml:"host_env"`
	TokenEnv    string `yaml:"token_env"`
	OrgEnv      string `yaml:"org_env"`
	Bucket      string `yaml:"bucket"`
	Measurement string `yaml:"measurement"`
}

type TimescaledbConfig struct {
	HostEnv     string `yaml:"host_env"`
	PortEnv     string `yaml:"port_env"`
	UserEnv     string `yaml:"user_env"`
	PasswordEnv string `yaml:"password_env"`
	DatabaseEnv string `yaml:"database_env"`
	Schema      string `yaml:"schema"`
	Table       string `yaml:"table"`
}

func (o OutputConfig) IsEnabled() bool {
	return o.Enabled == nil || *o.Enabled
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
	}

	return &wrapper.Storage, nil
}
