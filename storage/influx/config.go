package influx

import (
	"fmt"
)

type Config struct {
	HostEnv     string `yaml:"host_env"`
	TokenEnv    string `yaml:"token_env"`
	OrgEnv      string `yaml:"org_env"`
	Bucket      string `yaml:"bucket"`
	Measurement string `yaml:"measurement"`
}

func (c Config) Validate(outputName string) error {
	if outputName == "" {
		return fmt.Errorf("storage output name is empty")
	}
	if c.HostEnv == "" {
		return fmt.Errorf("storage.outputs[%s].influxdb2.host_env is empty", outputName)
	}
	if c.TokenEnv == "" {
		return fmt.Errorf("storage.outputs[%s].influxdb2.token_env is empty", outputName)
	}
	if c.OrgEnv == "" {
		return fmt.Errorf("storage.outputs[%s].influxdb2.org_env is empty", outputName)
	}
	if c.Bucket == "" {
		return fmt.Errorf("storage.outputs[%s].influxdb2.bucket is empty", outputName)
	}
	if c.Measurement == "" {
		return fmt.Errorf("storage.outputs[%s].influxdb2.measurement is empty", outputName)
	}
	return nil
}
