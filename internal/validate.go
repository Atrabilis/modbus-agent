package internal

import (
	"fmt"
	"sort"
)

// minWordsByDatatype lists every datatype decodeRegisterValue understands and
// the number of 16-bit words it needs when read as holding/input registers
// (function codes 0, 3 and 4). Text and raw types need at least one word.
var minWordsByDatatype = map[string]int{
	"U8":    1,
	"U16":   1,
	"S16":   1,
	"U32":   2,
	"S32":   2,
	"U32LE": 2,
	"S32LE": 2,
	"F32BE": 2,
	"U64BE": 4,
	"S64BE": 4,
	"STR":   1,
	"UTF8":  1,
	"HEX":   1,
}

// maxCoilsByDatatype lists the datatypes decodeCoilsResponse can pack coils or
// discrete inputs into (function codes 1 and 2), with the maximum number of
// bits each one holds. Zero means no fixed limit (raw and text types).
var maxCoilsByDatatype = map[string]int{
	"U8":    8,
	"U16":   16,
	"S16":   16,
	"U32":   32,
	"S32":   32,
	"U32LE": 32,
	"S32LE": 32,
	"HEX":   0,
	"STR":   0,
	"UTF8":  0,
}

// SupportedDatatypes returns the datatypes accepted for holding/input
// registers, sorted. Tests use it to keep validation and decoding in sync.
func SupportedDatatypes() []string {
	out := make([]string, 0, len(minWordsByDatatype))
	for dt := range minWordsByDatatype {
		out = append(out, dt)
	}
	sort.Strings(out)
	return out
}

// RegisterIssue describes a configured register the agent cannot read or
// decode correctly.
type RegisterIssue struct {
	Device   string
	Slave    string
	Register Register
	Problem  string
}

func (i RegisterIssue) String() string {
	return fmt.Sprintf("device=%q slave=%q register=%d name=%q: %s",
		i.Device, i.Slave, i.Register.Register, i.Register.Name, i.Problem)
}

// ValidateRegister returns why reg cannot be decoded correctly, or "" if it can.
func ValidateRegister(reg Register) string {
	switch reg.FunctionCode {
	case 1, 2:
		maxCoils, ok := maxCoilsByDatatype[reg.Datatype]
		if !ok {
			return fmt.Sprintf("datatype %q is not supported for function_code=%d", reg.Datatype, reg.FunctionCode)
		}
		if reg.Words < 1 {
			return fmt.Sprintf("words=%d: function_code=%d needs at least 1 coil", reg.Words, reg.FunctionCode)
		}
		if maxCoils > 0 && reg.Words > maxCoils {
			return fmt.Sprintf("words=%d: datatype %q holds at most %d coils", reg.Words, reg.Datatype, maxCoils)
		}
		return ""
	case 0, 3, 4:
		minWords, ok := minWordsByDatatype[reg.Datatype]
		if !ok {
			return fmt.Sprintf("unknown datatype %q", reg.Datatype)
		}
		if reg.Words < minWords {
			return fmt.Sprintf("words=%d: datatype %q needs at least %d", reg.Words, reg.Datatype, minWords)
		}
		return ""
	default:
		return fmt.Sprintf("unsupported function_code=%d", reg.FunctionCode)
	}
}

// ValidateDevices checks every register and returns a copy of devices without
// the invalid ones, plus the list of problems found. Invalid registers are
// dropped instead of failing the whole run, so one bad entry does not stop the
// collection of every other register.
func ValidateDevices(devices Devices) (Devices, []RegisterIssue) {
	var issues []RegisterIssue
	out := Devices{Plant: devices.Plant, Devices: make([]DeviceItem, 0, len(devices.Devices))}
	for _, item := range devices.Devices {
		dev := item.Device
		slaves := make([]Slave, 0, len(dev.Slaves))
		for _, slave := range dev.Slaves {
			kept := make([]Register, 0, len(slave.Registers))
			for _, reg := range slave.Registers {
				if problem := ValidateRegister(reg); problem != "" {
					issues = append(issues, RegisterIssue{Device: dev.Name, Slave: slave.Name, Register: reg, Problem: problem})
					continue
				}
				kept = append(kept, reg)
			}
			slave.Registers = kept
			slaves = append(slaves, slave)
		}
		dev.Slaves = slaves
		out.Devices = append(out.Devices, DeviceItem{Device: dev})
	}
	return out, issues
}
