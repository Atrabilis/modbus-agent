package internal

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Devices struct {
	Devices []DeviceItem `yaml:"devices"`
}

type DeviceItem struct {
	Device Device `yaml:"device"`
}

type Device struct {
	Name        string             `yaml:"name"`
	IP          string             `yaml:"ip"`
	Port        int                `yaml:"port"`
	Flags       []string           `yaml:"flags,omitempty"`
	Tags        map[string]string  `yaml:"tags,omitempty"`
	Healthcheck *HealthcheckConfig `yaml:"healthcheck,omitempty"`
	Slaves      []Slave            `yaml:"slaves"`
}

type HealthcheckConfig struct {
	Enabled       *bool              `yaml:"enabled,omitempty"`
	OnFail        string             `yaml:"on_fail,omitempty"`
	SuccessPolicy string             `yaml:"success_policy,omitempty"`
	Probes        []HealthcheckProbe `yaml:"probes,omitempty"`
}

type HealthcheckProbe struct {
	Name         string `yaml:"name,omitempty"`
	SlaveID      int    `yaml:"slave_id"`
	FunctionCode int    `yaml:"function_code"`
	Register     int    `yaml:"register"`
	Words        int    `yaml:"words"`
	Offset       int    `yaml:"offset,omitempty"`
	TimeoutMs    int    `yaml:"timeout_ms,omitempty"`
	Retries      int    `yaml:"retries,omitempty"`
}

func (h *HealthcheckConfig) IsEnabled() bool {
	if h == nil {
		return false
	}
	return h.Enabled == nil || *h.Enabled
}

func (h *HealthcheckConfig) OnFailMode() string {
	if h == nil {
		return "skip_device"
	}
	switch strings.ToLower(strings.TrimSpace(h.OnFail)) {
	case "", "skip_device":
		return "skip_device"
	case "fail_run":
		return "fail_run"
	default:
		return "skip_device"
	}
}

func (h *HealthcheckConfig) SuccessPolicyMode() string {
	if h == nil {
		return "any"
	}
	switch strings.ToLower(strings.TrimSpace(h.SuccessPolicy)) {
	case "", "any":
		return "any"
	case "all":
		return "all"
	default:
		return "any"
	}
}

type Slave struct {
	Name      string            `yaml:"name"`
	SlaveID   int               `yaml:"slave_id"`
	Offset    int               `yaml:"offset"`
	Tags      map[string]string `yaml:"tags,omitempty"`
	Registers []Register        `yaml:"modbus_registers"`
}

type Register struct {
	Register     int               `yaml:"register"`
	FunctionCode int               `yaml:"function_code"`
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Words        int               `yaml:"words"`
	Datatype     string            `yaml:"datatype"`
	Unit         string            `yaml:"unit"`
	Gain         float64           `yaml:"gain"`
	Flags        RegisterFlag      `yaml:"flags,omitempty"`
	Tags         map[string]string `yaml:"tags,omitempty"`
}

// RegisterFlag supports heterogeneous keys used in YAML:
//   - module_number: 16
//   - module_label: "amp/freq/unbal"
type RegisterFlag struct {
	ModuleNumber int    `yaml:"module_number"`
	ModuleLabel  string `yaml:"module_label"`
}

func LoadRegisters(path string, devices *Devices) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, devices)
}

func ReadRegisters(client interface {
	ReadCoils(address, quantity uint16) (results []byte, err error)
	ReadHoldingRegisters(address, quantity uint16) (results []byte, err error)
	ReadInputRegisters(address, quantity uint16) (results []byte, err error)
}, reg Register, offset int) ([]byte, error) {
	address := uint16(reg.Register - offset)
	quantity := uint16(reg.Words)

	switch reg.FunctionCode {
	case 1:
		return client.ReadCoils(address, quantity)
	case 0, 3:
		return client.ReadHoldingRegisters(address, quantity)
	case 4:
		return client.ReadInputRegisters(address, quantity)
	default:
		return nil, fmt.Errorf("unsupported function_code=%d for register %d", reg.FunctionCode, reg.Register)
	}
}

func ExpectedResponseBytes(reg Register) int {
	switch reg.FunctionCode {
	case 1:
		if reg.Words <= 0 {
			return 0
		}
		return (reg.Words + 7) / 8
	default:
		return reg.Words * 2
	}
}

func DecodeResponseBytes(reg Register, resp []byte) ([]byte, error) {
	switch reg.FunctionCode {
	case 1:
		return decodeCoilsResponse(reg, resp)
	default:
		return resp, nil
	}
}

func decodeCoilsResponse(reg Register, resp []byte) ([]byte, error) {
	if reg.Words <= 0 {
		return nil, fmt.Errorf("invalid coil quantity=%d for register %d", reg.Words, reg.Register)
	}

	var bitmask uint32
	for i := 0; i < reg.Words; i++ {
		byteIndex := i / 8
		bitIndex := uint(i % 8)
		if byteIndex >= len(resp) {
			break
		}
		if resp[byteIndex]&(1<<bitIndex) != 0 {
			bitmask |= 1 << uint(i)
		}
	}

	switch reg.Datatype {
	case "U8":
		if reg.Words > 8 {
			return nil, fmt.Errorf("datatype=%q supports at most 8 coils, got %d at register %d", reg.Datatype, reg.Words, reg.Register)
		}
		return []byte{byte(bitmask)}, nil
	case "U16", "S16":
		if reg.Words > 16 {
			return nil, fmt.Errorf("datatype=%q supports at most 16 coils, got %d at register %d", reg.Datatype, reg.Words, reg.Register)
		}
		return []byte{byte(bitmask >> 8), byte(bitmask)}, nil
	case "U32", "S32":
		return []byte{byte(bitmask >> 24), byte(bitmask >> 16), byte(bitmask >> 8), byte(bitmask)}, nil
	case "U32LE", "S32LE":
		return []byte{byte(bitmask >> 8), byte(bitmask), byte(bitmask >> 24), byte(bitmask >> 16)}, nil
	case "HEX", "STR", "UTF8":
		return resp, nil
	default:
		if reg.Words == 1 {
			return []byte{0, byte(bitmask)}, nil
		}
		return nil, fmt.Errorf("datatype=%q is not supported for function_code=1 at register %d", reg.Datatype, reg.Register)
	}
}

// ---------- Byte-order helpers (big-endian by byte) ----------

func U8(b []byte) uint8 {
	if len(b) == 0 {
		return 0
	}
	if len(b) == 1 {
		return uint8(b[0])
	}
	return uint8(b[len(b)-1]) // low byte
}

func U16(b []byte) uint16 {
	if len(b) < 2 {
		return 0
	}
	return uint16(b[0])<<8 | uint16(b[1])
}

func U32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func S16(b []byte) int16 {
	return int16(U16(b)) // two's complement
}

func S32(b []byte) int32 {
	return int32(U32(b)) // two's complement
}
func UTF8(b []byte) string {
	// Trim at the first NUL byte if present.
	if i := bytes.IndexByte(b, 0x00); i >= 0 {
		b = b[:i]
	}
	// Remove trailing NUL padding if any remains.
	b = bytes.TrimRight(b, "\x00")
	return string(b)
}

func U32LE(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	low := U16(b[0:2])  // first word = low
	high := U16(b[2:4]) // second word = high
	return uint32(low) | (uint32(high) << 16)
}

func S32LE(b []byte) int32 { return int32(U32LE(b)) }

func F32BE(b []byte) float32 {
	if len(b) < 4 {
		return 0
	}
	return math.Float32frombits(U32(b))
}

func U64BE(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func S64BE(b []byte) int64 {
	return int64(U64BE(b))
}

// RawHex returns the raw register bytes as a hex string (e.g. for MLD blocks).
// For 21 words this yields up to 84 hex chars. Intended for analyst interpretation.
func RawHex(b []byte) string {
	const hex = "0123456789abcdef"
	if len(b) == 0 {
		return ""
	}
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

// MergeTags builds the merged tag map from device, slave, and register tags (YAML).
// Order: device < slave < register. Then defaults: device_name/slave_name/slave_id, unit from register.Unit,
// backward-compatible aliases device/slave, and legacy flags (module_number, module_label) if set.
// All tag values are strings.
func MergeTags(dev *Device, slave *Slave, reg *Register) map[string]string {
	out := make(map[string]string)
	for k, v := range dev.Tags {
		out[k] = v
	}
	for k, v := range slave.Tags {
		out[k] = v
	}
	for k, v := range reg.Tags {
		out[k] = v
	}
	// Canonical identifiers used by Timescale writer.
	if out["device_name"] == "" && dev.Name != "" {
		out["device_name"] = dev.Name
	}
	if out["slave_name"] == "" && slave.Name != "" {
		out["slave_name"] = slave.Name
	}
	if out["slave_id"] == "" {
		out["slave_id"] = strconv.Itoa(slave.SlaveID)
	}
	// Backward-compatible aliases for existing Influx dashboards/queries.
	if out["device"] == "" && out["device_name"] != "" {
		out["device"] = out["device_name"]
	}
	if out["slave"] == "" && out["slave_name"] != "" {
		out["slave"] = out["slave_name"]
	}
	if out["unit"] == "" && reg.Unit != "" {
		out["unit"] = reg.Unit
	}
	if reg.Flags.ModuleNumber != 0 {
		out["module_number"] = strconv.Itoa(reg.Flags.ModuleNumber)
	}
	if reg.Flags.ModuleLabel != "" {
		out["module_label"] = reg.Flags.ModuleLabel
	}
	return out
}
