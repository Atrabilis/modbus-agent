package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/goburrow/modbus"
)

func TestLoadRegistersParsesRootPlant(t *testing.T) {
	t.Parallel()

	yaml := `plant: lalcktur
devices:
  - device:
      name: "logo8"
      ip: "127.0.0.1"
      port: 502
      slaves: []
`

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}

	var cfg Devices
	if err := LoadRegisters(path, &cfg); err != nil {
		t.Fatalf("LoadRegisters failed: %v", err)
	}

	if cfg.Plant != "lalcktur" {
		t.Fatalf("expected plant lalcktur, got %q", cfg.Plant)
	}
}

func TestMergeTagsAddsPlantWhenMissing(t *testing.T) {
	t.Parallel()

	dev := &Device{Name: "dev1"}
	slave := &Slave{Name: "slave1", SlaveID: 3}
	reg := &Register{Name: "power"}

	tags := MergeTags("petorca", dev, slave, reg)

	if got := tags["plant"]; got != "petorca" {
		t.Fatalf("expected plant tag petorca, got %q", got)
	}
}

func TestMergeTagsDoesNotOverrideSpecificPlant(t *testing.T) {
	t.Parallel()

	dev := &Device{
		Name: "dev1",
		Tags: map[string]string{"plant": "override_plant"},
	}
	slave := &Slave{Name: "slave1", SlaveID: 3}
	reg := &Register{Name: "power"}

	tags := MergeTags("petorca", dev, slave, reg)

	if got := tags["plant"]; got != "override_plant" {
		t.Fatalf("expected plant override_plant, got %q", got)
	}
}

type fakeHealthcheckClient struct {
	handler     *modbus.TCPClientHandler
	lastSlaveID byte
	fail        bool
}

func (f *fakeHealthcheckClient) ReadCoils(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeHealthcheckClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeHealthcheckClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	f.lastSlaveID = f.handler.SlaveId
	if f.fail {
		return nil, fmt.Errorf("read failed")
	}
	return []byte{0, 1}, nil
}

func boolPtr(v bool) *bool {
	return &v
}

func TestRunSlaveHealthcheckUsesSlaveIDDefault(t *testing.T) {
	t.Parallel()

	dev := Device{
		Name: "dev1",
		Healthcheck: &HealthcheckConfig{
			Enabled: boolPtr(true),
			Probes: []HealthcheckProbe{{
				FunctionCode: 4,
				Register:     1,
				Words:        1,
			}},
		},
	}
	slave := Slave{Name: "slave1", SlaveID: 7}
	handler := modbus.NewTCPClientHandler("127.0.0.1:502")
	handler.Timeout = 5 * time.Second
	client := &fakeHealthcheckClient{handler: handler}

	ok := RunSlaveHealthcheck(dev, slave, handler, client)
	if !ok {
		t.Fatal("expected healthcheck to pass")
	}
	if client.lastSlaveID != byte(slave.SlaveID) {
		t.Fatalf("expected slave id %d used in healthcheck, got %d", slave.SlaveID, client.lastSlaveID)
	}
}
