package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/atrabilis/modbus-agent/internal"
)

// These tests walk the input domain of decodeRegisterValue and compare every
// result with an independent oracle (encoding/binary and Go's own integer and
// float conversions), so they check the conversion itself rather than a few
// sampled readings. 8- and 16-bit types are checked exhaustively; 32- and
// 64-bit types use boundary values plus fuzz targets (`go test` runs the seed
// corpus; `go test -fuzz=FuzzDecode32 ./cmd` explores further).

var testTS = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func decode(t *testing.T, datatype string, gain float64, raw []byte) (interface{}, bool) {
	t.Helper()
	var out strings.Builder
	reg := internal.Register{Register: 1, Name: "r", Datatype: datatype, Gain: gain}
	return decodeRegisterValue(testTS, reg, raw, &out)
}

func decodeFloat(t *testing.T, datatype string, gain float64, raw []byte) float64 {
	t.Helper()
	got, ok := decode(t, datatype, gain, raw)
	if !ok {
		t.Fatalf("%s: ok=false for raw % x", datatype, raw)
	}
	v, isFloat := got.(float64)
	if !isFloat {
		t.Fatalf("%s: got %T, want float64", datatype, got)
	}
	return v
}

func be16(v uint16) []byte { b := make([]byte, 2); binary.BigEndian.PutUint16(b, v); return b }
func be32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func be64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

// wordSwapped32 lays out a 32-bit value low word first, each word big-endian:
// the layout behind the "U32LE"/"S32LE" datatypes.
func wordSwapped32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint16(b[0:2], uint16(v))
	binary.BigEndian.PutUint16(b[2:4], uint16(v>>16))
	return b
}

// sameFloat treats NaN as equal to NaN and distinguishes +0 from -0.
func sameFloat(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	return math.Float64bits(a) == math.Float64bits(b)
}

// --- 8 and 16 bits: whole domain -----------------------------------------

func TestDecode16BitTypesExhaustive(t *testing.T) {
	for v := 0; v <= math.MaxUint16; v++ {
		raw := be16(uint16(v))
		if got, want := decodeFloat(t, "U16", 1, raw), float64(uint16(v)); got != want {
			t.Fatalf("U16 % x: got %v, want %v", raw, got, want)
		}
		if got, want := decodeFloat(t, "S16", 1, raw), float64(int16(uint16(v))); got != want {
			t.Fatalf("S16 % x: got %v, want %v", raw, got, want)
		}
		// U8 reads one register and keeps its low byte.
		if got, want := decodeFloat(t, "U8", 1, raw), float64(uint8(v)); got != want {
			t.Fatalf("U8 % x: got %v, want %v", raw, got, want)
		}
	}
}

func TestDecodeU8SingleByte(t *testing.T) {
	for v := 0; v <= math.MaxUint8; v++ {
		if got := decodeFloat(t, "U8", 1, []byte{byte(v)}); got != float64(v) {
			t.Fatalf("U8 %#x: got %v", v, got)
		}
	}
}

// --- 32 bits: boundaries plus fuzzing --------------------------------------

var boundaries32 = []uint32{
	0, 1, 2,
	0x00007FFF, 0x00008000, 0x0000FFFF, // 16-bit sign and word edges
	0x00010000, 0x00010001, 0x0001FFFF, // first values that use the high word
	0x7FFFFFFE, 0x7FFFFFFF, 0x80000000, 0x80000001, // 32-bit sign edge
	0xFFFF0000, 0xFFFFFFFE, 0xFFFFFFFF,
}

func check32(t *testing.T, v uint32) {
	t.Helper()
	bigEndian := be32(v)
	swapped := wordSwapped32(v)
	cases := []struct {
		datatype string
		raw      []byte
		want     float64
	}{
		{"U32", bigEndian, float64(v)},
		{"S32", bigEndian, float64(int32(v))},
		{"U32LE", swapped, float64(v)},
		{"S32LE", swapped, float64(int32(v))},
		{"F32BE", bigEndian, float64(math.Float32frombits(v))},
	}
	for _, c := range cases {
		if got := decodeFloat(t, c.datatype, 1, c.raw); !sameFloat(got, c.want) {
			t.Fatalf("%s %#08x (raw % x): got %v, want %v", c.datatype, v, c.raw, got, c.want)
		}
	}
}

func TestDecode32BitTypesBoundaries(t *testing.T) {
	for _, v := range boundaries32 {
		check32(t, v)
	}
}

func FuzzDecode32(f *testing.F) {
	for _, v := range boundaries32 {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v uint32) { check32(t, v) })
}

// --- 64 bits: boundaries plus fuzzing --------------------------------------

var boundaries64 = []uint64{
	0, 1,
	0xFFFFFFFF, 0x100000000, // 32-bit edge
	1 << 53, 1<<53 + 1, // float64 stops being exact above 2^53
	math.MaxInt64, 1 << 63, math.MaxUint64,
}

func check64(t *testing.T, v uint64) {
	t.Helper()
	raw := be64(v)
	if got, want := decodeFloat(t, "U64BE", 1, raw), float64(v); got != want {
		t.Fatalf("U64BE %#016x: got %v, want %v", v, got, want)
	}
	if got, want := decodeFloat(t, "S64BE", 1, raw), float64(int64(v)); got != want {
		t.Fatalf("S64BE %#016x: got %v, want %v", v, got, want)
	}
}

func TestDecode64BitTypesBoundaries(t *testing.T) {
	for _, v := range boundaries64 {
		check64(t, v)
	}
}

func FuzzDecode64(f *testing.F) {
	for _, v := range boundaries64 {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v uint64) { check64(t, v) })
}

// The value is returned as float64, so 64-bit counters are exact only up to
// 2^53. Documented here because an energy counter is a 64-bit integer.
func TestDecode64BitPrecisionLimit(t *testing.T) {
	if got := decodeFloat(t, "U64BE", 1, be64(1<<53)); got != 1<<53 {
		t.Fatalf("2^53 must be exact, got %v", got)
	}
	if got := decodeFloat(t, "U64BE", 1, be64(1<<53+1)); got != 1<<53 {
		t.Fatalf("2^53+1 is expected to round to 2^53, got %v", got)
	}
}

// --- float specials ---------------------------------------------------------

// NaN and infinities are returned as valid values (ok=true); whatever
// consumes the sample has to decide what to do with them.
func TestDecodeF32BESpecialValues(t *testing.T) {
	cases := map[string]uint32{
		"+0":                0x00000000,
		"-0":                0x80000000,
		"smallest denormal": 0x00000001,
		"max float32":       0x7F7FFFFF,
		"+Inf":              0x7F800000,
		"-Inf":              0xFF800000,
		"quiet NaN":         0x7FC00000,
	}
	for name, bits := range cases {
		got := decodeFloat(t, "F32BE", 1, be32(bits))
		if want := float64(math.Float32frombits(bits)); !sameFloat(got, want) {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
	}
}

// --- gain -----------------------------------------------------------------

func TestGainMultipliesTheRawValue(t *testing.T) {
	// Gains used in production configs, plus a negative raw value for signed types.
	gains := []float64{1, 0.1, 0.01, 0.001, 0.0001, 5, 1.5}
	for _, g := range gains {
		cases := []struct {
			datatype string
			raw      []byte
			base     float64
		}{
			{"U8", []byte{0x00, 200}, 200},
			{"U16", be16(60000), 60000},
			{"S16", be16(uint16(0xFC18)), -1000},
			{"U32", be32(4000000000), 4000000000},
			{"S32", be32(uint32(0xFFFFFC18)), -1000},
			{"U32LE", wordSwapped32(70000), 70000},
			{"S32LE", wordSwapped32(uint32(0xFFFFFC18)), -1000},
			{"F32BE", be32(math.Float32bits(12.5)), 12.5},
			{"U64BE", be64(123456789012), 123456789012},
			{"S64BE", be64(uint64(0xFFFFFFFFFFFFFC18)), -1000},
		}
		for _, c := range cases {
			if got, want := decodeFloat(t, c.datatype, g, c.raw), c.base*g; got != want {
				t.Fatalf("%s gain %v: got %v, want %v", c.datatype, g, got, want)
			}
		}
	}
}

// --- short buffers ----------------------------------------------------------

// A response shorter than the datatype needs decodes as 0 with ok=true. This
// documents current behavior; a 0 is indistinguishable from a real reading.
func TestDecodeShortBufferYieldsZero(t *testing.T) {
	need := map[string]int{
		"U8": 1, "U16": 2, "S16": 2,
		"U32": 4, "S32": 4, "U32LE": 4, "S32LE": 4, "F32BE": 4,
		"U64BE": 8, "S64BE": 8,
	}
	for datatype, n := range need {
		for length := 0; length < n; length++ {
			raw := make([]byte, length)
			for i := range raw {
				raw[i] = 0xFF
			}
			if got := decodeFloat(t, datatype, 1, raw); got != 0 {
				t.Fatalf("%s with %d of %d bytes: got %v, want 0", datatype, length, n, got)
			}
		}
	}
}

// --- text types -------------------------------------------------------------

func TestDecodeTextTypes(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{"no padding", []byte("ABCD"), "ABCD"},
		{"NUL padding is trimmed", append([]byte("AB"), 0, 0, 0, 0), "AB"},
		{"all NUL is empty", []byte{0, 0, 0, 0}, ""},
		{"empty buffer", []byte{}, ""},
		{"cut at the first NUL, even mid-string", []byte{'A', 0, 'B', 'C'}, "A"},
	}
	for _, datatype := range []string{"STR", "UTF8"} {
		for _, c := range cases {
			got, ok := decode(t, datatype, 1, c.raw)
			if !ok || got != c.want {
				t.Fatalf("%s %s: got (%q, %v), want %q", datatype, c.name, got, ok, c.want)
			}
		}
	}
}

// A PostgreSQL text column rejects invalid UTF-8, so each invalid byte becomes
// U+FFFD, the same replacement encoding/json applies on the shadow path.
func TestDecodeTextReplacesInvalidUTF8(t *testing.T) {
	cases := []struct {
		raw  []byte
		want string
	}{
		{[]byte{'O', 'K', 0xFF, 0xFE}, "OK��"},
		{[]byte{0xC3, 0xA1, 'b', 'c'}, "ábc"},        // valid 2-byte rune is kept
		{[]byte{'a', 0xC3, 'b'}, "a�b"},              // truncated multi-byte sequence
		{[]byte{0xE2, 0x82, 0xAC, 0xFF, 0, 0}, "€�"}, // valid rune, invalid byte, NUL padding
	}
	for _, datatype := range []string{"STR", "UTF8"} {
		for _, c := range cases {
			got, ok := decode(t, datatype, 1, c.raw)
			if !ok || got != c.want {
				t.Fatalf("%s % x: got (%q, %v), want %q", datatype, c.raw, got, ok, c.want)
			}
		}
	}
}

// For any input the decoded text is valid UTF-8, has no NUL, and is exactly
// what the JSON shadow payload would store for the same bytes.
func FuzzDecodeText(f *testing.F) {
	f.Add([]byte("SN0000000A1\x00\x00"))
	f.Add([]byte{'O', 'K', 0xFF, 0xFE})
	f.Add([]byte{0xED, 0xA0, 0x80}) // UTF-16 surrogate, invalid in UTF-8
	f.Add([]byte{0xF4, 0x90, 0x80, 0x80})
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, ok := decode(t, "STR", 1, raw)
		s, isString := got.(string)
		if !ok || !isString {
			t.Fatalf("got (%v, %v)", got, ok)
		}
		if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			t.Fatalf("result %q is not clean UTF-8", s)
		}
		cut := raw
		if i := bytes.IndexByte(cut, 0); i >= 0 {
			cut = cut[:i]
		}
		encoded, err := json.Marshal(string(cut))
		if err != nil {
			t.Fatal(err)
		}
		var viaJSON string
		if err := json.Unmarshal(encoded, &viaJSON); err != nil {
			t.Fatal(err)
		}
		if s != viaJSON {
			t.Fatalf("decoder %q differs from the JSON path %q for % x", s, viaJSON, raw)
		}
	})
}

func TestDecodeHEXAllByteValues(t *testing.T) {
	raw := make([]byte, 256)
	for i := range raw {
		raw[i] = byte(i)
	}
	got, ok := decode(t, "HEX", 1, raw)
	if want := hex.EncodeToString(raw); !ok || got != want {
		t.Fatalf("HEX: got (%v, %v), want %s", got, ok, want)
	}
	if got, ok := decode(t, "HEX", 1, nil); !ok || got != "" {
		t.Fatalf("HEX empty: got (%q, %v)", got, ok)
	}
}

// --- unknown datatypes ------------------------------------------------------

func TestDecodeUnknownDatatypeIsRejected(t *testing.T) {
	var out strings.Builder
	reg := internal.Register{Register: 40550, Name: "r", Datatype: "NOT_A_TYPE"}
	got, ok := decodeRegisterValue(testTS, reg, be16(1), &out)
	if ok || got != nil {
		t.Fatalf("got (%v, %v), want (nil, false)", got, ok)
	}
	if !strings.Contains(out.String(), `unknown datatype="NOT_A_TYPE"`) {
		t.Fatalf("missing unknown-datatype log line: %q", out.String())
	}
}
