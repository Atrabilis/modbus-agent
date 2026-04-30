package internal

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
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

func TestLoadRegistersParsesTransportMode(t *testing.T) {
	t.Parallel()

	yaml := `plant: petorca
devices:
  - device:
      name: "tracker"
      ip: "192.168.1.23"
      port: 4001
      mode: "rtu_over_tcp"
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

	if got := cfg.Devices[0].Device.TransportMode(); got != "rtu_over_tcp" {
		t.Fatalf("expected transport mode rtu_over_tcp, got %q", got)
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

type fakeSession struct {
	slaveID     byte
	lastSlaveID byte
	timeout     time.Duration
}

func (f *fakeSession) ReadCoils(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeSession) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return nil, nil
}

func (f *fakeSession) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	f.lastSlaveID = f.slaveID
	return []byte{0, 1}, nil
}

func (f *fakeSession) SetSlaveID(id byte) {
	f.slaveID = id
}

func (f *fakeSession) SlaveID() byte {
	return f.slaveID
}

func (f *fakeSession) SetTimeout(timeout time.Duration) {
	f.timeout = timeout
}

func (f *fakeSession) Timeout() time.Duration {
	return f.timeout
}

func (f *fakeSession) Close() error {
	return nil
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
	session := &fakeSession{timeout: 5 * time.Second}

	ok := RunSlaveHealthcheck(dev, slave, session)
	if !ok {
		t.Fatal("expected healthcheck to pass")
	}
	if session.lastSlaveID != byte(slave.SlaveID) {
		t.Fatalf("expected slave id %d used in healthcheck, got %d", slave.SlaveID, session.lastSlaveID)
	}
}

func TestRunSlaveHealthcheckSkipsWhenSlaveRequestsIt(t *testing.T) {
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
	slave := Slave{Name: "slave1", SlaveID: 7, SkipHealthcheck: true}
	session := &fakeSession{timeout: 5 * time.Second}

	ok := RunSlaveHealthcheck(dev, slave, session)
	if !ok {
		t.Fatal("expected skipped healthcheck to pass")
	}
	if session.lastSlaveID != 0 {
		t.Fatalf("expected no healthcheck read when skip_healthcheck is true, got slave id %d", session.lastSlaveID)
	}
}

func TestRTUOverTCPSessionReadHoldingRegisters(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		req := make([]byte, 8)
		if _, err := io.ReadFull(conn, req); err != nil {
			return
		}

		resp := []byte{7, 3, 2, 0x12, 0x34}
		crc := crc16(resp)
		resp = append(resp, byte(crc), byte(crc>>8))
		_, _ = conn.Write(resp)
	}()

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("atoi port: %v", err)
	}

	session, err := NewPollSession(Device{
		Name: "tracker",
		IP:   host,
		Port: port,
		Mode: "rtu_over_tcp",
	})
	if err != nil {
		t.Fatalf("new poll session: %v", err)
	}
	defer session.Close()
	session.SetSlaveID(7)

	resp, err := session.ReadHoldingRegisters(151, 1)
	if err != nil {
		t.Fatalf("ReadHoldingRegisters failed: %v", err)
	}
	if len(resp) != 2 || resp[0] != 0x12 || resp[1] != 0x34 {
		t.Fatalf("unexpected payload: % x", resp)
	}
}
