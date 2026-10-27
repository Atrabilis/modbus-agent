package main

// Tests for pollDevice against fake Modbus endpoints listening on 127.0.0.1.
// Regular cases use the server from github.com/simonvetter/modbus with a
// handler we control (values, exceptions, delays). Two cases it cannot
// produce are served by minimal hand-written servers below: a response with
// the wrong transaction id, and Modbus RTU frames over a raw TCP socket.

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/atrabilis/modbus-agent/internal"
	mbserver "github.com/simonvetter/modbus"
)

// fakeDevice is the register map served by the fake Modbus TCP server.
type fakeDevice struct {
	mu        sync.Mutex
	holding   map[uint8]map[uint16]uint16
	input     map[uint8]map[uint16]uint16
	coils     map[uint8]map[uint16]bool
	failAddr  map[uint16]bool // reads covering any of these addresses get an exception
	delay     time.Duration   // applied to every request
	requested int
}

func newFakeDevice() *fakeDevice {
	return &fakeDevice{
		holding:  map[uint8]map[uint16]uint16{},
		input:    map[uint8]map[uint16]uint16{},
		coils:    map[uint8]map[uint16]bool{},
		failAddr: map[uint16]bool{},
	}
}

func (f *fakeDevice) setHolding(unit uint8, addr uint16, words ...uint16) {
	if f.holding[unit] == nil {
		f.holding[unit] = map[uint16]uint16{}
	}
	for i, w := range words {
		f.holding[unit][addr+uint16(i)] = w
	}
}

func (f *fakeDevice) setInput(unit uint8, addr uint16, words ...uint16) {
	if f.input[unit] == nil {
		f.input[unit] = map[uint16]uint16{}
	}
	for i, w := range words {
		f.input[unit][addr+uint16(i)] = w
	}
}

func (f *fakeDevice) setCoils(unit uint8, addr uint16, bits ...bool) {
	if f.coils[unit] == nil {
		f.coils[unit] = map[uint16]bool{}
	}
	for i, b := range bits {
		f.coils[unit][addr+uint16(i)] = b
	}
}

// begin applies the configured delay and rejects reads that touch a failing
// address. Unknown addresses read as zero, like many real devices do.
func (f *fakeDevice) begin(addr, quantity uint16) error {
	f.mu.Lock()
	f.requested++
	delay := f.delay
	fail := false
	for a := addr; a < addr+quantity; a++ {
		if f.failAddr[a] {
			fail = true
		}
	}
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if fail {
		return mbserver.ErrIllegalDataAddress
	}
	return nil
}

func readWords(m map[uint16]uint16, addr, quantity uint16) []uint16 {
	out := make([]uint16, quantity)
	for i := range out {
		out[i] = m[addr+uint16(i)]
	}
	return out
}

func (f *fakeDevice) HandleCoils(req *mbserver.CoilsRequest) ([]bool, error) {
	if req.IsWrite {
		return nil, mbserver.ErrIllegalFunction
	}
	if err := f.begin(req.Addr, req.Quantity); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bool, req.Quantity)
	for i := range out {
		out[i] = f.coils[req.UnitId][req.Addr+uint16(i)]
	}
	return out, nil
}

func (f *fakeDevice) HandleDiscreteInputs(req *mbserver.DiscreteInputsRequest) ([]bool, error) {
	return nil, mbserver.ErrIllegalFunction
}

func (f *fakeDevice) HandleHoldingRegisters(req *mbserver.HoldingRegistersRequest) ([]uint16, error) {
	if req.IsWrite {
		return nil, mbserver.ErrIllegalFunction
	}
	if err := f.begin(req.Addr, req.Quantity); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return readWords(f.holding[req.UnitId], req.Addr, req.Quantity), nil
}

func (f *fakeDevice) HandleInputRegisters(req *mbserver.InputRegistersRequest) ([]uint16, error) {
	if err := f.begin(req.Addr, req.Quantity); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return readWords(f.input[req.UnitId], req.Addr, req.Quantity), nil
}

// freePort returns a TCP port that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startModbusServer serves dev over Modbus TCP and returns its port. The
// library does not report the port it bound to, so a free port is reserved
// first and the start is retried if another process takes it meanwhile.
func startModbusServer(t *testing.T, dev *fakeDevice) int {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		port := freePort(t)
		srv, err := mbserver.NewServer(&mbserver.ServerConfiguration{
			URL:        fmt.Sprintf("tcp://127.0.0.1:%d", port),
			Timeout:    30 * time.Second,
			MaxClients: 10,
			Logger:     log.New(io.Discard, "", 0),
		}, dev)
		if err != nil {
			t.Fatalf("new modbus server: %v", err)
		}
		if err := srv.Start(); err != nil {
			lastErr = err
			continue
		}
		t.Cleanup(func() { _ = srv.Stop() })
		return port
	}
	t.Fatalf("start modbus server: %v", lastErr)
	return 0
}

func f32Words(v float32) []uint16 {
	bits := math.Float32bits(v)
	return []uint16{uint16(bits >> 16), uint16(bits)}
}

func u64Words(v uint64) []uint16 {
	return []uint16{uint16(v >> 48), uint16(v >> 32), uint16(v >> 16), uint16(v)}
}

// fieldsBySlave flattens samples into slave name -> field name -> value,
// leaving out the raw_* hex copies.
func fieldsBySlave(t *testing.T, samples []sample) map[string]map[string]interface{} {
	t.Helper()
	out := map[string]map[string]interface{}{}
	for _, s := range samples {
		slave := s.Tags["slave_name"]
		if out[slave] == nil {
			out[slave] = map[string]interface{}{}
		}
		for k, v := range s.Fields {
			if len(k) > 4 && k[:4] == "raw_" {
				continue
			}
			if _, dup := out[slave][k]; dup {
				t.Fatalf("field %s of slave %s emitted twice", k, slave)
			}
			out[slave][k] = v
		}
	}
	return out
}

// testDevice is a two-slave device whose registers are served by fakeDevice
// from seedTestDevice. Slave inv2 uses offset 1, as many production configs do.
func testDevice(port int) internal.Device {
	return internal.Device{
		Name:      "inverter_block",
		IP:        "127.0.0.1",
		Port:      port,
		TimeoutMs: 1000,
		Slaves: []internal.Slave{
			{
				Name:    "inv1",
				SlaveID: 1,
				Registers: []internal.Register{
					{Register: 100, FunctionCode: 3, Name: "ac_voltage", Words: 1, Datatype: "U16", Gain: 0.1},
					{Register: 101, FunctionCode: 3, Name: "temperature", Words: 1, Datatype: "S16", Gain: 1},
					{Register: 102, FunctionCode: 3, Name: "energy", Words: 2, Datatype: "U32", Gain: 1},
					{Register: 104, FunctionCode: 3, Name: "frequency", Words: 2, Datatype: "F32BE", Gain: 1},
					{Register: 106, FunctionCode: 3, Name: "total_wh", Words: 4, Datatype: "U64BE", Gain: 1},
					{Register: 120, FunctionCode: 3, Name: "model", Words: 2, Datatype: "STR", Gain: 1},
					{Register: 200, FunctionCode: 4, Name: "status", Words: 1, Datatype: "U16", Gain: 1},
					{Register: 0, FunctionCode: 1, Name: "relays", Words: 8, Datatype: "U8", Gain: 1},
				},
			},
			{
				Name:    "inv2",
				SlaveID: 2,
				Offset:  1,
				Registers: []internal.Register{
					{Register: 4951, FunctionCode: 3, Name: "ac_voltage", Words: 1, Datatype: "U16", Gain: 1},
				},
			},
		},
	}
}

func seedTestDevice(dev *fakeDevice) {
	dev.setHolding(1, 100, 2345)           // 234.5 with gain 0.1
	dev.setHolding(1, 101, 0xFF38)         // -200 as S16
	dev.setHolding(1, 102, 0x0001, 0x1170) // 70000
	dev.setHolding(1, 104, f32Words(50.25)...)
	dev.setHolding(1, 106, u64Words(1<<40+5)...)
	dev.setHolding(1, 120, 0x4142, 0x3132) // "AB12"
	dev.setInput(1, 200, 7)
	dev.setCoils(1, 0, true, false, true) // bits 0 and 2 set -> 5
	dev.setHolding(2, 4950, 42)           // register 4951 with offset 1
}

func TestPollDeviceReadsAndDecodesEveryRegister(t *testing.T) {
	t.Parallel()

	dev := newFakeDevice()
	seedTestDevice(dev)
	port := startModbusServer(t, dev)

	samples, out, requests, failed, err := pollDevice("plant_x", testDevice(port), time.Unix(1_800_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("pollDevice: %v\n%s", err, out)
	}
	if failed != 0 {
		t.Fatalf("expected no failed requests, got %d\n%s", failed, out)
	}
	// inv1: holding 100-109 and 120-121 are separate blocks (gap > 0), plus
	// input 200 and coils 0-7; inv2: one block. Five requests in total.
	if requests != 5 {
		t.Fatalf("expected 5 Modbus requests, got %d\n%s", requests, out)
	}

	got := fieldsBySlave(t, samples)
	want := map[string]map[string]interface{}{
		"inv1": {
			"ac_voltage":  234.5,
			"temperature": -200.0,
			"energy":      70000.0,
			"frequency":   50.25,
			"total_wh":    float64(1<<40 + 5),
			"model":       "AB12",
			"status":      7.0,
			"relays":      5.0,
		},
		"inv2": {"ac_voltage": 42.0},
	}
	for slave, fields := range want {
		for name, wantValue := range fields {
			gotValue, ok := got[slave][name]
			if !ok {
				t.Errorf("%s.%s missing\n%s", slave, name, out)
				continue
			}
			if gv, isFloat := gotValue.(float64); isFloat {
				if math.Abs(gv-wantValue.(float64)) > 1e-9 {
					t.Errorf("%s.%s = %v, want %v", slave, name, gv, wantValue)
				}
				continue
			}
			if gotValue != wantValue {
				t.Errorf("%s.%s = %v, want %v", slave, name, gotValue, wantValue)
			}
		}
	}

	for _, s := range samples {
		for _, tag := range []string{"plant", "device_name", "slave_name", "slave_id"} {
			if s.Tags[tag] == "" {
				t.Fatalf("sample without %s tag: %v", tag, s.Tags)
			}
		}
		if s.Tags["plant"] != "plant_x" || s.Tags["device_name"] != "inverter_block" {
			t.Fatalf("unexpected tags: %v", s.Tags)
		}
	}
}

func TestPollDeviceKeepsOtherBlocksWhenOneRaisesAnException(t *testing.T) {
	t.Parallel()

	dev := newFakeDevice()
	seedTestDevice(dev)
	dev.failAddr[120] = true // the STR block
	port := startModbusServer(t, dev)

	samples, out, _, failed, err := pollDevice("plant_x", testDevice(port), time.Now().UTC())
	if err != nil {
		t.Fatalf("pollDevice: %v", err)
	}
	if failed != 1 {
		t.Fatalf("expected 1 failed request, got %d\n%s", failed, out)
	}
	got := fieldsBySlave(t, samples)
	if _, ok := got["inv1"]["model"]; ok {
		t.Fatal("model should be missing when its block raises an exception")
	}
	if got["inv1"]["ac_voltage"] != 234.5 || got["inv2"]["ac_voltage"] != 42.0 {
		t.Fatalf("other blocks should still be read, got %v\n%s", got, out)
	}
}

func healthcheck(onFail string, register int) *internal.HealthcheckConfig {
	enabled := true
	return &internal.HealthcheckConfig{
		Enabled: &enabled,
		OnFail:  onFail,
		Probes:  []internal.HealthcheckProbe{{FunctionCode: 3, Register: register, Words: 1, TimeoutMs: 300}},
	}
}

func TestPollDeviceHealthcheckSkipsFailingSlave(t *testing.T) {
	t.Parallel()

	dev := newFakeDevice()
	seedTestDevice(dev)
	dev.failAddr[9000] = true
	port := startModbusServer(t, dev)

	cfg := testDevice(port)
	cfg.Healthcheck = healthcheck("skip_device", 9000)
	cfg.Slaves[1].SkipHealthcheck = true

	samples, out, _, _, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err != nil {
		t.Fatalf("skip_device must not fail the run: %v", err)
	}
	got := fieldsBySlave(t, samples)
	if len(got["inv1"]) != 0 {
		t.Fatalf("inv1 failed its healthcheck and should be skipped, got %v\n%s", got["inv1"], out)
	}
	if got["inv2"]["ac_voltage"] != 42.0 {
		t.Fatalf("inv2 skips the healthcheck and should be read, got %v\n%s", got["inv2"], out)
	}
}

func TestPollDeviceHealthcheckFailRunReturnsError(t *testing.T) {
	t.Parallel()

	dev := newFakeDevice()
	dev.failAddr[9000] = true
	port := startModbusServer(t, dev)

	cfg := testDevice(port)
	cfg.Healthcheck = healthcheck("fail_run", 9000)

	samples, _, _, _, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err == nil {
		t.Fatal("fail_run healthcheck should return an error")
	}
	if len(samples) != 0 {
		t.Fatalf("expected no samples, got %d", len(samples))
	}
}

func TestPollDeviceTimeoutProducesNoSamples(t *testing.T) {
	t.Parallel()

	dev := newFakeDevice()
	seedTestDevice(dev)
	dev.delay = 400 * time.Millisecond
	port := startModbusServer(t, dev)

	cfg := testDevice(port)
	cfg.TimeoutMs = 100
	cfg.Slaves = cfg.Slaves[1:] // one block is enough

	samples, out, requests, failed, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err != nil {
		t.Fatalf("a read timeout is not a connection error: %v", err)
	}
	if len(samples) != 0 || requests != 1 || failed != 1 {
		t.Fatalf("expected 0 samples, 1 request, 1 failed; got %d, %d, %d\n%s", len(samples), requests, failed, out)
	}
}

func TestPollDeviceUnreachableEndpointReturnsError(t *testing.T) {
	t.Parallel()

	cfg := testDevice(freePort(t)) // nothing listens there
	samples, _, _, _, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err == nil {
		t.Fatal("expected a connection error")
	}
	if len(samples) != 0 {
		t.Fatalf("expected no samples, got %d", len(samples))
	}
}

// startMismatchServer answers Modbus TCP read requests for holding registers
// with value, but the first answer carries a wrong transaction id.
func startMismatchServer(t *testing.T, value uint16) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for n := 0; ; n++ {
			req := make([]byte, 12) // MBAP (7) + fc, addr, quantity (5)
			if _, err := io.ReadFull(conn, req); err != nil {
				return
			}
			tid := binary.BigEndian.Uint16(req[0:2])
			if n == 0 {
				tid += 100
			}
			quantity := binary.BigEndian.Uint16(req[10:12])
			pdu := []byte{req[7], byte(2 * quantity)}
			for i := uint16(0); i < quantity; i++ {
				pdu = binary.BigEndian.AppendUint16(pdu, value)
			}
			resp := binary.BigEndian.AppendUint16(nil, tid)
			resp = append(resp, 0, 0)
			resp = binary.BigEndian.AppendUint16(resp, uint16(len(pdu)+1))
			resp = append(resp, req[6])
			resp = append(resp, pdu...)
			if _, err := conn.Write(resp); err != nil {
				return
			}
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestPollDeviceRetriesTransactionIDMismatch(t *testing.T) {
	t.Parallel()

	port := startMismatchServer(t, 321)
	cfg := internal.Device{
		Name: "gateway", IP: "127.0.0.1", Port: port, TimeoutMs: 1000,
		Slaves: []internal.Slave{{Name: "meter", SlaveID: 1, Registers: []internal.Register{
			{Register: 10, FunctionCode: 3, Name: "power", Words: 1, Datatype: "U16", Gain: 1},
		}}},
	}

	samples, out, requests, _, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err != nil {
		t.Fatalf("pollDevice: %v", err)
	}
	if requests != 2 {
		t.Fatalf("expected the read to be retried once (2 requests), got %d\n%s", requests, out)
	}
	if got := fieldsBySlave(t, samples)["meter"]["power"]; got != 321.0 {
		t.Fatalf("power = %v, want 321 after the retry\n%s", got, out)
	}
}

func TestPollDeviceWithoutTransactionIDRetryKeepsTheError(t *testing.T) {
	t.Parallel()

	port := startMismatchServer(t, 321)
	zero := 0
	cfg := internal.Device{
		Name: "gateway", IP: "127.0.0.1", Port: port, TimeoutMs: 1000, TransactionIDRetries: &zero,
		Slaves: []internal.Slave{{Name: "meter", SlaveID: 1, Registers: []internal.Register{
			{Register: 10, FunctionCode: 3, Name: "power", Words: 1, Datatype: "U16", Gain: 1},
		}}},
	}

	samples, _, requests, _, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err != nil {
		t.Fatalf("pollDevice: %v", err)
	}
	if requests != 1 || len(samples) != 0 {
		t.Fatalf("with retries disabled expected 1 request and no samples, got %d and %d", requests, len(samples))
	}
}

func rtuCRC(b []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, v := range b {
		crc ^= uint16(v)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

// startRTUServer answers Modbus RTU read-holding-register frames sent over a
// raw TCP socket, like a serial gateway in "RTU over TCP" mode. Registers
// absent from regs read as zero.
func startRTUServer(t *testing.T, slaveID byte, regs map[uint16]uint16) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			req := make([]byte, 8)
			if _, err := io.ReadFull(conn, req); err != nil {
				return
			}
			if binary.LittleEndian.Uint16(req[6:]) != rtuCRC(req[:6]) || req[0] != slaveID || req[1] != 3 {
				return
			}
			addr := binary.BigEndian.Uint16(req[2:4])
			quantity := binary.BigEndian.Uint16(req[4:6])
			resp := []byte{slaveID, 3, byte(2 * quantity)}
			for i := uint16(0); i < quantity; i++ {
				resp = binary.BigEndian.AppendUint16(resp, regs[addr+i])
			}
			resp = binary.LittleEndian.AppendUint16(resp, rtuCRC(resp))
			if _, err := conn.Write(resp); err != nil {
				return
			}
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestPollDeviceRTUOverTCP(t *testing.T) {
	t.Parallel()

	port := startRTUServer(t, 5, map[uint16]uint16{151: 0xFFF6, 152: 1200})
	cfg := internal.Device{
		Name: "tracker", IP: "127.0.0.1", Port: port, Mode: "rtu_over_tcp", TimeoutMs: 1000,
		Slaves: []internal.Slave{{Name: "tcu_1", SlaveID: 5, Registers: []internal.Register{
			{Register: 151, FunctionCode: 3, Name: "actual_angle_deg", Words: 1, Datatype: "S16", Gain: 0.1},
			{Register: 152, FunctionCode: 3, Name: "motor_current", Words: 1, Datatype: "U16", Gain: 0.01},
		}}},
	}

	samples, out, requests, failed, err := pollDevice("plant_x", cfg, time.Now().UTC())
	if err != nil {
		t.Fatalf("pollDevice: %v\n%s", err, out)
	}
	if requests != 1 || failed != 0 {
		t.Fatalf("expected one block read without failures, got %d requests, %d failed\n%s", requests, failed, out)
	}
	got := fieldsBySlave(t, samples)["tcu_1"]
	if math.Abs(got["actual_angle_deg"].(float64)-(-1.0)) > 1e-9 || math.Abs(got["motor_current"].(float64)-12.0) > 1e-9 {
		t.Fatalf("unexpected values %v\n%s", got, out)
	}
}
