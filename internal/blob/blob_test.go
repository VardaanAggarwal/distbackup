package blob

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestComputeMatchesSHA256(t *testing.T) {
	data := []byte("distbackup")
	want := sha256.Sum256(data)
	got := Compute(data)
	if got != ID(want) {
		t.Fatalf("Compute = %s, want %x", got, want)
	}
}

func TestRoundTripHex(t *testing.T) {
	id := Compute([]byte("hello"))
	parsed, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if parsed != id {
		t.Fatalf("round trip mismatch: %s != %s", parsed, id)
	}
}

func TestParseIDRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"too short":     "abcd",
		"too long":      Compute([]byte("x")).String() + "00",
		"not hex":       "zz" + Compute([]byte("x")).String()[2:],
		"odd length":    Compute([]byte("x")).String()[:63],
		"uppercase ok?": "", // placeholder replaced below
	}
	delete(cases, "uppercase ok?")

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseID(in); err == nil {
				t.Fatalf("ParseID(%q) = nil error, want error", in)
			}
		})
	}
}

func TestZeroID(t *testing.T) {
	var id ID
	if !id.IsZero() {
		t.Fatal("zero value should report IsZero")
	}
	if Compute(nil).IsZero() {
		t.Fatal("SHA-256 of empty input should not equal the zero ID")
	}
}

// IDs must serialise as hex strings, not as base64 byte arrays. A repository
// format is a long-lived contract; JSON that is unreadable by a human makes
// every future debugging session harder.
func TestJSONEncodingIsHex(t *testing.T) {
	id := Compute([]byte("payload"))

	encoded, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `"` + id.String() + `"`
	if string(encoded) != want {
		t.Fatalf("Marshal = %s, want %s", encoded, want)
	}

	var decoded ID
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded != id {
		t.Fatalf("JSON round trip mismatch: %s != %s", decoded, id)
	}
}

func TestJSONDecodeRejectsBadLength(t *testing.T) {
	var id ID
	if err := json.Unmarshal([]byte(`"abcd"`), &id); err == nil {
		t.Fatal("Unmarshal of short hex = nil error, want error")
	}
}

func TestVerify(t *testing.T) {
	data := []byte("some blob contents")
	id := Compute(data)

	if !Verify(id, data) {
		t.Fatal("Verify rejected matching data")
	}

	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[0] ^= 0xFF
	if Verify(id, corrupted) {
		t.Fatal("Verify accepted corrupted data")
	}
}

func TestShortAndPrefix(t *testing.T) {
	id := Compute([]byte("x"))
	if len(id.Short()) != 12 {
		t.Fatalf("Short() = %q, want 12 hex chars", id.Short())
	}
	if len(id.Prefix()) != 2 {
		t.Fatalf("Prefix() = %q, want 2 hex chars", id.Prefix())
	}
	if id.String()[:2] != id.Prefix() {
		t.Fatalf("Prefix() = %q, want first 2 chars of %q", id.Prefix(), id.String())
	}
	if id.String()[:12] != id.Short() {
		t.Fatalf("Short() = %q, want first 12 chars of %q", id.Short(), id.String())
	}
}

// The ID type must be usable as a map key without conversion. This is the
// property the whole index design depends on; a change to []byte would
// compile everywhere else and quietly cost an allocation per lookup.
func TestIDIsMapKey(t *testing.T) {
	m := map[ID]int{}
	m[Compute([]byte("a"))] = 1
	m[Compute([]byte("b"))] = 2
	m[Compute([]byte("a"))] = 3

	if len(m) != 2 {
		t.Fatalf("len = %d, want 2", len(m))
	}
	if m[Compute([]byte("a"))] != 3 {
		t.Fatal("map lookup by recomputed ID failed")
	}
}

func BenchmarkCompute64KiB(b *testing.B) {
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		_ = Compute(data)
	}
}
