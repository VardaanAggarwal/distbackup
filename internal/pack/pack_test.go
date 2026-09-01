package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

func testBlob(seed int64, size int) []byte {
	r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	b := make([]byte, size)
	r.Read(b) //nolint:errcheck // rand.Read never fails
	return b
}

// buildPack writes n blobs into a pack and returns the bytes, the pack ID and
// the blobs keyed by ID.
func buildPack(t *testing.T, n int) ([]byte, blob.ID, map[blob.ID][]byte) {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	blobs := make(map[blob.ID][]byte, n)

	for i := range n {
		data := testBlob(int64(i), 1024+i*7)
		id := blob.Compute(data)
		added, err := w.Add(id, data)
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if !added {
			t.Fatalf("blob %d reported as duplicate", i)
		}
		blobs[id] = data
	}

	id, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return buf.Bytes(), id, blobs
}

func TestRoundTrip(t *testing.T) {
	data, _, blobs := buildPack(t, 20)

	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := len(r.Entries()); got != len(blobs) {
		t.Fatalf("pack has %d entries, want %d", got, len(blobs))
	}

	for id, want := range blobs {
		got, err := r.ReadBlob(id)
		if err != nil {
			t.Fatalf("ReadBlob(%s): %v", id.Short(), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("blob %s round-tripped incorrectly", id.Short())
		}
	}

	n, err := r.VerifyAll()
	if err != nil {
		t.Fatalf("VerifyAll: %v", err)
	}
	if n != len(blobs) {
		t.Fatalf("VerifyAll checked %d blobs, want %d", n, len(blobs))
	}
}

// The pack ID must be the hash of the pack's own bytes, which is what makes
// an upload idempotent under retry.
func TestPackIDIsContentAddress(t *testing.T) {
	data, id, _ := buildPack(t, 5)
	if want := blob.Compute(data); id != want {
		t.Fatalf("pack ID = %s, want SHA-256 of the pack bytes %s", id.Short(), want.Short())
	}
}

// Writing the same blobs twice must produce byte-identical packs, or a retry
// after a partial upload would write a different object under a different key.
func TestPackIsDeterministic(t *testing.T) {
	a, idA, _ := buildPack(t, 8)
	b, idB, _ := buildPack(t, 8)

	if !bytes.Equal(a, b) {
		t.Fatal("two identical writes produced different pack bytes")
	}
	if idA != idB {
		t.Fatalf("pack IDs differ: %s vs %s", idA.Short(), idB.Short())
	}
}

// A blob whose declared ID does not match its bytes must be rejected at write
// time. Catching it here means the failure is traceable; catching it at
// restore time means it is not.
func TestAddRejectsMismatchedID(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	data := testBlob(1, 512)
	wrongID := blob.Compute([]byte("something else"))

	_, err := w.Add(wrongID, data)
	if err == nil {
		t.Fatal("Add accepted a blob that does not hash to its declared ID")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

// Two chunk workers can both miss the index for the same content and hand the
// same blob to the writer. The writer is the last place to catch it.
func TestAddDeduplicatesWithinPack(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	data := testBlob(3, 2048)
	id := blob.Compute(data)

	added, err := w.Add(id, data)
	if err != nil || !added {
		t.Fatalf("first Add: added=%v err=%v", added, err)
	}
	added, err = w.Add(id, data)
	if err != nil {
		t.Fatalf("second Add: %v", err)
	}
	if added {
		t.Fatal("duplicate blob reported as added")
	}
	if w.Count() != 1 {
		t.Fatalf("Count = %d, want 1", w.Count())
	}
	if !w.Contains(id) {
		t.Fatal("Contains returned false for a blob that was added")
	}
}

func TestAddRejectsZeroID(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	if _, err := w.Add(blob.Zero, []byte("x")); err == nil {
		t.Fatal("Add accepted the zero ID")
	}
}

func TestFinishRejectsEmptyPack(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	if _, _, err := w.Finish(); err == nil {
		t.Fatal("Finish wrote an empty pack")
	}
}

func TestWriterRejectsUseAfterFinish(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	data := testBlob(1, 128)
	if _, err := w.Add(blob.Compute(data), data); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if _, err := w.Add(blob.Compute(data), data); err == nil {
		t.Fatal("Add succeeded after Finish")
	}
	if _, _, err := w.Finish(); err == nil {
		t.Fatal("Finish succeeded twice")
	}
}

// Truncation is the most likely real-world corruption: an interrupted upload
// or a partial write. It must be detected, not silently tolerated.
func TestTruncationIsDetected(t *testing.T) {
	data, _, _ := buildPack(t, 10)

	for _, cut := range []int{1, 4, TrailerSize, TrailerSize + 1, len(data) / 2, len(data) - 1} {
		t.Run(fmt.Sprintf("truncated_to_%d", len(data)-cut), func(t *testing.T) {
			truncated := data[:len(data)-cut]
			_, err := NewReader(bytes.NewReader(truncated), int64(len(truncated)))
			if err == nil {
				t.Fatal("reader accepted a truncated pack")
			}
		})
	}
}

func TestBadMagicIsDetected(t *testing.T) {
	data, _, _ := buildPack(t, 3)
	corrupted := bytes.Clone(data)
	corrupted[len(corrupted)-1] ^= 0xFF

	_, err := NewReader(bytes.NewReader(corrupted), int64(len(corrupted)))
	if err == nil {
		t.Fatal("reader accepted a pack with bad magic")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

// A corrupted length field must not become a huge allocation. This is the
// guard that turns a malformed file into an error instead of an OOM.
func TestAbsurdHeaderLengthIsRejected(t *testing.T) {
	data, _, _ := buildPack(t, 3)
	corrupted := bytes.Clone(data)

	lenOff := len(corrupted) - TrailerSize
	binary.LittleEndian.PutUint32(corrupted[lenOff:lenOff+4], 0xFFFFFFFF)

	_, err := NewReader(bytes.NewReader(corrupted), int64(len(corrupted)))
	if err == nil {
		t.Fatal("reader accepted an absurd header length")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func TestZeroHeaderLengthIsRejected(t *testing.T) {
	data, _, _ := buildPack(t, 3)
	corrupted := bytes.Clone(data)

	lenOff := len(corrupted) - TrailerSize
	binary.LittleEndian.PutUint32(corrupted[lenOff:lenOff+4], 0)

	if _, err := NewReader(bytes.NewReader(corrupted), int64(len(corrupted))); err == nil {
		t.Fatal("reader accepted a zero header length")
	}
}

// Corrupting a blob's bytes must be caught on read. This is the property that
// makes a backup trustworthy: it fails loudly rather than returning wrong data.
func TestCorruptedBlobIsDetectedOnRead(t *testing.T) {
	data, _, blobs := buildPack(t, 6)

	corrupted := bytes.Clone(data)
	corrupted[100] ^= 0xFF // inside the first blob's data region

	r, err := NewReader(bytes.NewReader(corrupted), int64(len(corrupted)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// VerifyAll must find it.
	if _, err := r.VerifyAll(); err == nil {
		t.Fatal("VerifyAll accepted a pack with a corrupted blob")
	} else if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}

	// And so must a direct read of the affected blob.
	found := false
	for id := range blobs {
		if _, err := r.ReadBlob(id); err != nil {
			if !errs.IsCorrupt(err) {
				t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no blob reported corruption despite a flipped byte")
	}
}

func TestReadBlobNotFound(t *testing.T) {
	data, _, _ := buildPack(t, 3)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	missing := blob.Compute([]byte("not in this pack"))
	_, err = r.ReadBlob(missing)
	if !errs.IsNotFound(err) {
		t.Fatalf("kind = %s, want not_found", errs.KindOf(err))
	}
	if _, ok := r.Lookup(missing); ok {
		t.Fatal("Lookup found a blob that is not present")
	}
}

// The index-free recovery path: one ranged GET of the tail must be enough to
// learn a pack's contents. This is the whole justification for D-007.
func TestParseTailRecoversWithoutIndex(t *testing.T) {
	data, _, blobs := buildPack(t, 15)

	tail := data
	if len(data) > SuggestedTailSize {
		tail = data[len(data)-SuggestedTailSize:]
	}

	hdr, err := ParseTail(tail, int64(len(data)))
	if err != nil {
		t.Fatalf("ParseTail: %v", err)
	}
	if len(hdr.Entries) != len(blobs) {
		t.Fatalf("recovered %d entries, want %d", len(hdr.Entries), len(blobs))
	}
	for _, e := range hdr.Entries {
		if _, ok := blobs[e.ID]; !ok {
			t.Fatalf("recovered unknown blob %s", e.ID.Short())
		}
	}
}

// When the tail window was too small, the error must say exactly how many
// bytes are needed, so the retry is guaranteed to succeed rather than being
// another guess.
func TestParseTailReportsRequiredSize(t *testing.T) {
	data, _, _ := buildPack(t, 50)

	tooShort := data[len(data)-TrailerSize-10:]
	_, err := ParseTail(tooShort, int64(len(data)))

	var short *ErrShortTail
	if !errors.As(err, &short) {
		t.Fatalf("got %v, want *ErrShortTail", err)
	}
	if short.Need <= int64(len(tooShort)) {
		t.Fatalf("Need = %d, must exceed the %d bytes supplied", short.Need, len(tooShort))
	}

	// The reported size must actually be sufficient.
	retry := data[int64(len(data))-short.Need:]
	if _, err := ParseTail(retry, int64(len(data))); err != nil {
		t.Fatalf("ParseTail with the reported size failed: %v", err)
	}
}

// NewReader must transparently perform the second ranged read when the first
// tail guess is too small.
func TestNewReaderRetriesShortTail(t *testing.T) {
	// Enough blobs that the header exceeds SuggestedTailSize.
	var buf bytes.Buffer
	w := NewWriter(&buf)
	const n = 1200
	ids := make([]blob.ID, 0, n)
	for i := range n {
		data := testBlob(int64(i), 64)
		id := blob.Compute(data)
		if _, err := w.Add(id, data); err != nil {
			t.Fatalf("Add: %v", err)
		}
		ids = append(ids, id)
	}
	if _, _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	data := buf.Bytes()
	hdrLen := binary.LittleEndian.Uint32(data[len(data)-TrailerSize : len(data)-TrailerSize+4])
	if int(hdrLen)+TrailerSize <= SuggestedTailSize {
		t.Skipf("header is %d bytes, not large enough to exercise the retry path", hdrLen)
	}

	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if len(r.Entries()) != n {
		t.Fatalf("got %d entries, want %d", len(r.Entries()), n)
	}
	if _, ok := r.Lookup(ids[0]); !ok {
		t.Fatal("first blob missing after a short-tail retry")
	}
}

// An unknown format version must be refused rather than guessed at. A reader
// that tries to interpret a future version is how a format becomes
// un-evolvable.
func TestUnknownVersionIsRefused(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	data := testBlob(1, 256)
	if _, err := w.Add(blob.Compute(data), data); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Rewrite the header with a future version number.
	raw := buf.Bytes()
	hdrLen := int(binary.LittleEndian.Uint32(raw[len(raw)-TrailerSize : len(raw)-TrailerSize+4]))
	hdrStart := len(raw) - TrailerSize - hdrLen
	patched := bytes.Replace(raw[hdrStart:len(raw)-TrailerSize],
		[]byte(`"version":1`), []byte(`"version":9`), 1)
	if len(patched) != hdrLen {
		t.Skip("patched header changed length; cannot rewrite in place")
	}

	corrupted := bytes.Clone(raw)
	copy(corrupted[hdrStart:], patched)

	_, err := NewReader(bytes.NewReader(corrupted), int64(len(corrupted)))
	if err == nil {
		t.Fatal("reader accepted an unknown format version")
	}
	if errs.KindOf(err) != errs.KindUnsupported {
		t.Fatalf("kind = %s, want unsupported", errs.KindOf(err))
	}
}

// An entry pointing past the end of the blob data must be rejected before any
// read is attempted against it.
func TestOutOfBoundsEntryIsRejected(t *testing.T) {
	hdr := Header{
		Version: FormatVersion,
		Entries: []Entry{{ID: blob.Compute([]byte("x")), Offset: 0, Length: 1 << 30}},
	}
	tail := encodeTail(t, hdr)

	_, err := ParseTail(tail, int64(len(tail)))
	if err == nil {
		t.Fatal("ParseTail accepted an entry that spans past the blob data")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func TestNegativeEntryIsRejected(t *testing.T) {
	hdr := Header{
		Version: FormatVersion,
		Entries: []Entry{{ID: blob.Compute([]byte("x")), Offset: -1, Length: 10}},
	}
	tail := encodeTail(t, hdr)

	if _, err := ParseTail(tail, int64(len(tail))); err == nil {
		t.Fatal("ParseTail accepted a negative offset")
	}
}

// encodeTail builds just the header+trailer portion of a pack, for tests that
// need to construct a malformed header directly.
func encodeTail(t *testing.T, hdr Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// Reuse the writer's own encoding path by adding one real blob and then
	// discarding the data region.
	data := []byte("placeholder")
	if _, err := w.Add(blob.Compute(data), data); err != nil {
		t.Fatalf("Add: %v", err)
	}
	w.entries = hdr.Entries
	if _, _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return buf.Bytes()
}

func TestIsFullAndSize(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	if w.IsFull(1024) {
		t.Fatal("empty writer reports full")
	}

	data := testBlob(1, 2048)
	if _, err := w.Add(blob.Compute(data), data); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if w.Size() != 2048 {
		t.Fatalf("Size = %d, want 2048", w.Size())
	}
	if !w.IsFull(1024) {
		t.Fatal("writer with 2048 bytes does not report full at a 1024 target")
	}
}

func TestWriteErrorPropagates(t *testing.T) {
	sentinel := errors.New("disk full")
	w := NewWriter(failingWriter{err: sentinel})

	data := testBlob(1, 128)
	_, err := w.Add(blob.Compute(data), data)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

type failingWriter struct{ err error }

func (f failingWriter) Write([]byte) (int, error) { return 0, f.err }

func BenchmarkPackWrite(b *testing.B) {
	blobs := make([][]byte, 200)
	ids := make([]blob.ID, len(blobs))
	total := 0
	for i := range blobs {
		blobs[i] = testBlob(int64(i), 72*1024)
		ids[i] = blob.Compute(blobs[i])
		total += len(blobs[i])
	}

	b.SetBytes(int64(total))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		var buf bytes.Buffer
		buf.Grow(total + 64*1024)
		w := NewWriter(&buf)
		for i := range blobs {
			if _, err := w.Add(ids[i], blobs[i]); err != nil {
				b.Fatal(err)
			}
		}
		if _, _, err := w.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPackReadBlob(b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	ids := make([]blob.ID, 200)
	for i := range ids {
		data := testBlob(int64(i), 72*1024)
		ids[i] = blob.Compute(data)
		if _, err := w.Add(ids[i], data); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := w.Finish(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(72 * 1024)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := r.ReadBlob(ids[i%len(ids)]); err != nil {
			b.Fatal(err)
		}
	}
}
