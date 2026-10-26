package internal

import (
	"strings"
	"testing"
)

func TestValidateRegisterHoldingRegisters(t *testing.T) {
	// Each supported datatype is valid with its minimum word count and invalid
	// with one word less, for every function code that reads 16-bit words.
	for _, fc := range []int{0, 3, 4} {
		for dt, minWords := range minWordsByDatatype {
			ok := Register{Register: 100, Name: "r", FunctionCode: fc, Datatype: dt, Words: minWords}
			if problem := ValidateRegister(ok); problem != "" {
				t.Fatalf("fc=%d %s words=%d: unexpected problem %q", fc, dt, minWords, problem)
			}
			short := ok
			short.Words = minWords - 1
			if problem := ValidateRegister(short); !strings.Contains(problem, "needs at least") {
				t.Fatalf("fc=%d %s words=%d: got %q, want a words problem", fc, dt, short.Words, problem)
			}
		}
	}
}

func TestValidateRegisterRejects(t *testing.T) {
	cases := []struct {
		name string
		reg  Register
		want string
	}{
		// Typos found in production configs on 2026-10-26.
		{"U64 is not a datatype (use U64BE)", Register{FunctionCode: 3, Datatype: "U64", Words: 4}, `unknown datatype "U64"`},
		{"I16 is not a datatype (use S16)", Register{FunctionCode: 3, Datatype: "I16", Words: 1}, `unknown datatype "I16"`},
		{"datatypes are case sensitive", Register{FunctionCode: 3, Datatype: "u16", Words: 1}, `unknown datatype "u16"`},
		{"empty datatype", Register{FunctionCode: 3, Datatype: "", Words: 1}, `unknown datatype ""`},
		{"32-bit value in one word", Register{FunctionCode: 3, Datatype: "U32", Words: 1}, "needs at least 2"},
		{"64-bit value in two words", Register{FunctionCode: 4, Datatype: "S64BE", Words: 2}, "needs at least 4"},
		{"function code 5 is a write", Register{FunctionCode: 5, Datatype: "U16", Words: 1}, "unsupported function_code=5"},
		{"negative function code", Register{FunctionCode: -1, Datatype: "U16", Words: 1}, "unsupported function_code=-1"},
		{"float cannot hold coils", Register{FunctionCode: 1, Datatype: "F32BE", Words: 1}, "not supported for function_code=1"},
		{"U8 holds at most 8 coils", Register{FunctionCode: 1, Datatype: "U8", Words: 9}, "at most 8 coils"},
		{"U16 holds at most 16 inputs", Register{FunctionCode: 2, Datatype: "U16", Words: 17}, "at most 16 coils"},
		{"zero coils", Register{FunctionCode: 2, Datatype: "U16", Words: 0}, "needs at least 1 coil"},
	}
	for _, c := range cases {
		if got := ValidateRegister(c.reg); !strings.Contains(got, c.want) {
			t.Fatalf("%s: got %q, want it to contain %q", c.name, got, c.want)
		}
	}
}

func TestValidateRegisterAcceptsCoils(t *testing.T) {
	cases := []Register{
		{FunctionCode: 1, Datatype: "U16", Words: 1}, // the form used in production
		{FunctionCode: 2, Datatype: "U16", Words: 1},
		{FunctionCode: 1, Datatype: "U8", Words: 8},
		{FunctionCode: 2, Datatype: "S16", Words: 16},
		{FunctionCode: 1, Datatype: "U32LE", Words: 32},
		{FunctionCode: 2, Datatype: "HEX", Words: 100},
	}
	for _, reg := range cases {
		if problem := ValidateRegister(reg); problem != "" {
			t.Fatalf("%+v: unexpected problem %q", reg, problem)
		}
	}
}

func TestValidateDevicesDropsOnlyInvalidRegisters(t *testing.T) {
	devices := Devices{
		Plant: "plant_a",
		Devices: []DeviceItem{{Device: Device{
			Name: "logger",
			Slaves: []Slave{
				{Name: "inv1", Registers: []Register{
					{Register: 1, Name: "power", FunctionCode: 3, Datatype: "S32", Words: 2},
					{Register: 3, Name: "co2", FunctionCode: 3, Datatype: "U64", Words: 4},
					{Register: 7, Name: "temp", FunctionCode: 3, Datatype: "S16", Words: 1},
				}},
				{Name: "inv2", Registers: []Register{
					{Register: 1, Name: "power", FunctionCode: 3, Datatype: "S32", Words: 1},
				}},
			},
		}}},
	}

	valid, issues := ValidateDevices(devices)

	if valid.Plant != "plant_a" {
		t.Fatalf("plant not preserved: %q", valid.Plant)
	}
	slaves := valid.Devices[0].Device.Slaves
	if len(slaves) != 2 {
		t.Fatalf("slaves must be kept even when all their registers are invalid, got %d", len(slaves))
	}
	if got := []string{slaves[0].Registers[0].Name, slaves[0].Registers[1].Name}; got[0] != "power" || got[1] != "temp" || len(slaves[0].Registers) != 2 {
		t.Fatalf("inv1 registers: got %+v, want power and temp in order", slaves[0].Registers)
	}
	if len(slaves[1].Registers) != 0 {
		t.Fatalf("inv2 registers: got %+v, want none", slaves[1].Registers)
	}
	if len(issues) != 2 {
		t.Fatalf("got %d issues, want 2: %v", len(issues), issues)
	}
	if s := issues[0].String(); !strings.Contains(s, `device="logger" slave="inv1" register=3 name="co2"`) {
		t.Fatalf("issue text: %q", s)
	}
	// The input must not be modified.
	if len(devices.Devices[0].Device.Slaves[0].Registers) != 3 {
		t.Fatalf("ValidateDevices modified its input")
	}
}
