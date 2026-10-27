package postgres_shadow

import (
	"encoding/json"
	"math"
	"testing"
)

// A device can send IEEE NaN or infinity in a float register. Those values must
// not break the JSON payload (which would drop every field of the row); they
// travel as the strings PostgreSQL accepts for double precision.
func TestCollectPayloadFieldsKeepsRowWithNonFiniteFloats(t *testing.T) {
	fields := map[string]interface{}{
		"current_a":      math.NaN(),
		"voltage_ab":     math.Inf(1),
		"power_factor":   math.Inf(-1),
		"frequency":      50.02,
		"energy_counter": float64(46914732),
		"serial_id":      "SN0000000A1",
		"raw_current_a":  "7fc00000", // raw_* fields are never stored
	}

	payload := collectPayloadFields(fields)
	encoded, err := json.Marshal(map[string]interface{}{"fields": payload})
	if err != nil {
		t.Fatalf("payload with non-finite floats must encode, got: %v", err)
	}

	var decoded struct {
		Fields map[string]interface{} `json:"fields"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := map[string]interface{}{
		"current_a":      "NaN",
		"voltage_ab":     "Infinity",
		"power_factor":   "-Infinity",
		"frequency":      50.02,
		"energy_counter": float64(46914732), // whole numbers are sent as integers
		"serial_id":      "SN0000000A1",
	}
	if len(decoded.Fields) != len(want) {
		t.Fatalf("got %d fields %v, want %d", len(decoded.Fields), decoded.Fields, len(want))
	}
	for name, w := range want {
		if got := decoded.Fields[name]; got != w {
			t.Fatalf("field %s: got %#v, want %#v", name, got, w)
		}
	}
}

func TestNormalizeValue(t *testing.T) {
	cases := []struct {
		in   interface{}
		want interface{}
	}{
		{math.NaN(), "NaN"},
		{math.Inf(1), "Infinity"},
		{math.Inf(-1), "-Infinity"},
		{float64(42), int64(42)},
		{-2147483648.0, int64(-2147483648)}, // device sentinels are kept as-is
		{12.5, 12.5},
		{"text", "text"},
		{nil, nil},
	}
	for _, c := range cases {
		if got := normalizeValue(c.in); got != c.want {
			t.Fatalf("normalizeValue(%#v) = %#v, want %#v", c.in, got, c.want)
		}
	}
}
