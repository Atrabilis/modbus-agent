package postgres_shadow

import "fmt"

type Config struct {
	HostEnv     string `yaml:"host_env"`
	PortEnv     string `yaml:"port_env"`
	UserEnv     string `yaml:"user_env"`
	PasswordEnv string `yaml:"password_env"`
	DatabaseEnv string `yaml:"database_env"`
	Schema      string `yaml:"schema"`
	Table       string `yaml:"table"`
}

func (c Config) Validate(outputName string) error {
	if outputName == "" {
		return fmt.Errorf("storage output name is empty")
	}
	if c.HostEnv == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.host_env is empty", outputName)
	}
	if c.PortEnv == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.port_env is empty", outputName)
	}
	if c.UserEnv == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.user_env is empty", outputName)
	}
	if c.PasswordEnv == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.password_env is empty", outputName)
	}
	if c.DatabaseEnv == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.database_env is empty", outputName)
	}
	if c.Schema == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.schema is empty", outputName)
	}
	if c.Table == "" {
		return fmt.Errorf("storage.outputs[%s].postgres_shadow.table is empty", outputName)
	}
	return nil
}
