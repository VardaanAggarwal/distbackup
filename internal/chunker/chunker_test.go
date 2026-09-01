package chunker

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"sort"
	"testing"
)

// deterministicData returns n bytes from a fixed seed, so every failure is
// reproducible and no test depends on the machine's entropy.
func deterministicData(n int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	data := make([]byte, n)
	r.Read(data) //nolint:errcheck // rand.Read never fails
	return data
}

// boundaries returns the cumulative end offsets of each chunk.
func boundaries(t *testing.T, data []byte, cfg Config) []int {
	t.Helper()
	chunks, err := Split(data, cfg)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	offs := make([]int, 0, len(chunks))
	pos := 0
	for _, c := range chunks {
		pos += c.Len()
		offs = append(offs, pos)
	}
	return offs
}

// TestBoundaryShift is mandatory (docs/ENGINEERING-RULES.md R6) and must never be weakened.
//
// It is the test that proves this is content-defined chunking rather than
// fixed-size chunking wearing a costume. Insert one byte at the front of a
// large buffer: with fixed-size chunking every subsequent boundary moves and
// the match rate is ~0%. With working CDC the boundaries realign almost
// immediately and the rate is very high.
func TestBoundaryShift(t *testing.T) {
	const size = 12 << 20 // comfortably above the required 10 MiB
	cfg := DefaultConfig()

	original := deterministicData(size, 42)

	// Insert a single byte at the very front, so every boundary in the
	// original lies after the insertion point.
	shifted := make([]byte, 0, size+1)
	shifted = append(shifted, 0xAB)
	shifted = append(shifted, original...)

	origOffsets := boundaries(t, original, cfg)
	shiftedOffsets := boundaries(t, shifted, cfg)

	origSet := make(map[int]struct{}, len(origOffsets))
	for _, o := range origOffsets {
		origSet[o] = struct{}{}
	}

	// A boundary at offset b in the shifted stream sits at content position
	// b-1 in the original.
	matched := 0
	for _, b := range shiftedOffsets {
		if _, ok := origSet[b-1]; ok {
			matched++
		}
	}

	rate := float64(matched) / float64(len(shiftedOffsets)) * 100

	t.Logf("original chunks: %d, shifted chunks: %d, matched boundaries: %d (%.2f%%)",
		len(origOffsets), len(shiftedOffsets), matched, rate)

	if rate < 95.0 {
		t.Fatalf("boundary match rate %.2f%% < 95%%: chunking does not resynchronise after an insertion", rate)
	}
}

// Fixed-size chunking must fail the same measurement. Without this, a bug
// that turned the chunker into a fixed-size splitter could still pass
// TestBoundaryShift if the assertion were ever miscomputed — this pins the
// measurement itself, not just the implementation.
func TestBoundaryShiftMeasurementDetectsFixedChunking(t *testing.T) {
	const size = 2 << 20
	const fixed = 64 * 1024

	original := deterministicData(size, 7)

	fixedBoundaries := func(data []byte) map[int]struct{} {
		set := make(map[int]struct{})
		for off := fixed; off <= len(data); off += fixed {
			set[off] = struct{}{}
		}
		return set
	}

	origSet := fixedBoundaries(original)
	shiftedSet := fixedBoundaries(append([]byte{0xAB}, original...))

	matched := 0
	for b := range shiftedSet {
		if _, ok := origSet[b-1]; ok {
			matched++
		}
	}
	rate := float64(matched) / float64(len(shiftedSet)) * 100
	if rate >= 95.0 {
		t.Fatalf("fixed-size chunking scored %.2f%%; the measurement is not discriminating", rate)
	}
	t.Logf("fixed-size chunking scores %.2f%% on the same measurement, as expected", rate)
}

// A middle insertion must leave boundaries before it completely untouched and
// resynchronise after it. This is the property that makes incremental backup
// of a large edited file cheap.
func TestBoundaryShiftMidStream(t *testing.T) {
	const size = 8 << 20
	const insertAt = 4 << 20
	cfg := DefaultConfig()

	original := deterministicData(size, 99)

	modified := make([]byte, 0, size+1)
	modified = append(modified, original[:insertAt]...)
	modified = append(modified, 0xCD)
	modified = append(modified, original[insertAt:]...)

	origOffsets := boundaries(t, original, cfg)
	modOffsets := boundaries(t, modified, cfg)

	origSet := make(map[int]struct{}, len(origOffsets))
	for _, o := range origOffsets {
		origSet[o] = struct{}{}
	}

	// Before the insertion the streams are byte-identical, so boundaries must
	// match exactly with no offset.
	beforeTotal, beforeMatched := 0, 0
	for _, b := range modOffsets {
		if b >= insertAt {
			continue
		}
		beforeTotal++
		if _, ok := origSet[b]; ok {
			beforeMatched++
		}
	}
	if beforeMatched != beforeTotal {
		t.Fatalf("boundaries before the insertion point changed: %d/%d matched", beforeMatched, beforeTotal)
	}

	// After it, they must realign at a one-byte offset.
	afterTotal, afterMatched := 0, 0
	for _, b := range modOffsets {
		if b <= insertAt+1 {
			continue
		}
		afterTotal++
		if _, ok := origSet[b-1]; ok {
			afterMatched++
		}
	}
	rate := float64(afterMatched) / float64(afterTotal) * 100
	t.Logf("after mid-stream insertion: %d/%d boundaries realigned (%.2f%%)", afterMatched, afterTotal, rate)
	if rate < 95.0 {
		t.Fatalf("post-insertion realignment %.2f%% < 95%%", rate)
	}
}

// Chunks must concatenate back to exactly the input. A chunker that drops or
// duplicates a byte would corrupt every backup, and would not necessarily be
// caught by a boundary test.
func TestSplitIsLossless(t *testing.T) {
	for _, size := range []int{0, 1, 100, DefaultMinSize - 1, DefaultMinSize, DefaultAvgSize, DefaultMaxSize, DefaultMaxSize + 1, 5 << 20} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			data := deterministicData(size, int64(size)+1)
			chunks, err := Split(data, DefaultConfig())
			if err != nil {
				t.Fatalf("Split: %v", err)
			}

			var rebuilt []byte
			var expectedOffset int64
			for _, c := range chunks {
				if c.Offset != expectedOffset {
					t.Fatalf("chunk offset = %d, want %d", c.Offset, expectedOffset)
				}
				expectedOffset += int64(c.Len())
				rebuilt = append(rebuilt, c.Data...)
			}

			if !bytes.Equal(rebuilt, data) {
				t.Fatalf("reassembled %d bytes, want %d, content equal = %v",
					len(rebuilt), len(data), bytes.Equal(rebuilt, data))
			}
		})
	}
}

// Every chunk but the last must respect [MinSize, MaxSize]. The last may be
// short because the stream simply ended.
func TestChunkSizeBounds(t *testing.T) {
	cfg := DefaultConfig()
	data := deterministicData(10<<20, 5)

	chunks, err := Split(data, cfg)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("got %d chunks, want several", len(chunks))
	}

	for i, c := range chunks[:len(chunks)-1] {
		if c.Len() < cfg.MinSize {
			t.Fatalf("chunk %d is %d bytes, below MinSize %d", i, c.Len(), cfg.MinSize)
		}
		if c.Len() > cfg.MaxSize {
			t.Fatalf("chunk %d is %d bytes, above MaxSize %d", i, c.Len(), cfg.MaxSize)
		}
	}
	if last := chunks[len(chunks)-1]; last.Len() > cfg.MaxSize {
		t.Fatalf("final chunk is %d bytes, above MaxSize %d", last.Len(), cfg.MaxSize)
	}
}

// The same input must always produce the same chunks. If this ever fails,
// deduplication is broken in a way that would show up as a backup that never
// stops growing.
func TestDeterminism(t *testing.T) {
	data := deterministicData(4<<20, 11)

	first, err := Split(data, DefaultConfig())
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	second, err := Split(data, DefaultConfig())
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	if len(first) != len(second) {
		t.Fatalf("chunk counts differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i].Data, second[i].Data) {
			t.Fatalf("chunk %d differs between runs", i)
		}
	}
}

// Chunking must not depend on how the underlying reader segments its output.
// A network reader returns short reads; a file reader usually does not. If
// those produced different boundaries, dedup would depend on transport.
func TestIndependentOfReadSizes(t *testing.T) {
	data := deterministicData(3<<20, 13)

	want, err := Split(data, DefaultConfig())
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	// A reader that returns awkward, varying short reads.
	c, err := New(&chunkyReader{data: data, sizes: []int{1, 7, 4096, 13, 65535, 3}}, DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var got []Chunk
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, ch)
	}

	if len(got) != len(want) {
		t.Fatalf("chunk counts differ: short reads %d, single read %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("chunk %d differs when the reader returns short reads", i)
		}
	}
}

type chunkyReader struct {
	data  []byte
	pos   int
	sizes []int
	next  int
}

func (r *chunkyReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	size := r.sizes[r.next%len(r.sizes)]
	r.next++
	if size > len(p) {
		size = len(p)
	}
	if r.pos+size > len(r.data) {
		size = len(r.data) - r.pos
	}
	n := copy(p[:size], r.data[r.pos:r.pos+size])
	r.pos += n
	return n, nil
}

// A read error must surface rather than being silently treated as EOF, which
// would truncate a backup and report success.
func TestReadErrorPropagates(t *testing.T) {
	sentinel := errors.New("disk on fire")
	c, err := New(&failingReader{err: sentinel}, DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.Next()
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

type failingReader struct{ err error }

func (r *failingReader) Read([]byte) (int, error) { return 0, r.err }

// TestChunkSizeDistribution measures the real distribution rather than
// assuming it. D-004 claims normalization pulls sizes toward the target; this
// is the evidence for that claim, and the numbers it logs are the only ones
// allowed to appear in the README (docs/ENGINEERING-RULES.md R2).
func TestChunkSizeDistribution(t *testing.T) {
	cfg := DefaultConfig()
	data := deterministicData(64<<20, 2024)

	chunks, err := Split(data, cfg)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(chunks) < 100 {
		t.Fatalf("only %d chunks; sample too small to characterise", len(chunks))
	}

	// Drop the final chunk: it is truncated by end-of-stream, not by content.
	sizes := make([]int, 0, len(chunks)-1)
	total := 0
	for _, c := range chunks[:len(chunks)-1] {
		sizes = append(sizes, c.Len())
		total += c.Len()
	}
	sort.Ints(sizes)

	mean := float64(total) / float64(len(sizes))
	pct := func(p float64) int { return sizes[int(float64(len(sizes)-1)*p)] }
	atMax := 0
	for _, s := range sizes {
		if s == cfg.MaxSize {
			atMax++
		}
	}

	t.Logf("chunk size distribution over %d MiB, %d chunks:", 64, len(sizes))
	t.Logf("  mean   %8.0f bytes (%.2f KiB), target %d KiB", mean, mean/1024, cfg.AvgSize/1024)
	t.Logf("  min    %8d", sizes[0])
	t.Logf("  p10    %8d", pct(0.10))
	t.Logf("  p50    %8d", pct(0.50))
	t.Logf("  p90    %8d", pct(0.90))
	t.Logf("  p99    %8d", pct(0.99))
	t.Logf("  max    %8d", sizes[len(sizes)-1])
	t.Logf("  forced cuts at MaxSize: %d (%.2f%%)", atMax, float64(atMax)/float64(len(sizes))*100)

	// Guard rails, deliberately loose: the point of this test is to report the
	// distribution, not to pin it to a value that a legitimate tuning change
	// would break. What must hold is that the mean is in the right
	// neighbourhood of the target rather than an order of magnitude away.
	if mean < float64(cfg.AvgSize)*0.5 || mean > float64(cfg.AvgSize)*2.0 {
		t.Fatalf("mean chunk size %.0f is not within 0.5x-2x of the %d target", mean, cfg.AvgSize)
	}
}

// The Gear table is part of the on-disk format: changing it changes every
// boundary and silently destroys deduplication against existing repositories.
// This pins it.
func TestGearTableIsPinned(t *testing.T) {
	h := sha256.New()
	var buf [8]byte
	for _, v := range gearTable {
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:]) //nolint:errcheck // hash.Write never returns an error
	}
	got := hex.EncodeToString(h.Sum(nil))

	// Golden value produced by this same code on 2026-08-26. If this test
	// fails, the Gear table changed. Do not update this constant to make the
	// test pass unless you intend to break dedup against every existing
	// repository — that is a format break and needs a version bump.
	const want = "d29093e6712e90f0bc37a7f04c5354412a53527b1c94cba195bdba6a7158a4d1"
	if got != want {
		t.Fatalf("gear table checksum = %s, want %s: the table changed, which breaks dedup", got, want)
	}
}

func TestGearTableHasNoDuplicates(t *testing.T) {
	seen := make(map[uint64]int, 256)
	for i, v := range gearTable {
		if prev, ok := seen[v]; ok {
			t.Fatalf("gear table entries %d and %d are both %#x", prev, i, v)
		}
		seen[v] = i
	}
	if len(seen) != 256 {
		t.Fatalf("gear table has %d distinct values, want 256", len(seen))
	}
}

func TestSpreadMaskBitCounts(t *testing.T) {
	for nbits := 1; nbits <= maskHi-maskLo+1; nbits++ {
		m := spreadMask(nbits)
		if got := bits.OnesCount64(m); got != nbits {
			t.Errorf("spreadMask(%d) has %d bits set, want %d (mask %#016x)", nbits, got, m, nbits)
		}
		if m&((1<<maskLo)-1) != 0 {
			t.Errorf("spreadMask(%d) = %#016x sets bits below %d", nbits, m, maskLo)
		}
	}
}

// The strict mask must make boundaries rarer than the loose one, or
// normalization is inverted and the distribution would be pushed away from
// the target instead of toward it.
func TestStrictMaskIsStricterThanLoose(t *testing.T) {
	c, err := New(newByteReader(nil), DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	strictBits := bits.OnesCount64(c.maskS)
	looseBits := bits.OnesCount64(c.maskL)
	if strictBits <= looseBits {
		t.Fatalf("maskS has %d bits and maskL has %d; strict must test more bits", strictBits, looseBits)
	}
	if want := 2 * DefaultNormalization; strictBits-looseBits != want {
		t.Fatalf("mask bit difference = %d, want %d", strictBits-looseBits, want)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]Config{
		"zero min":           {MinSize: 0, AvgSize: 1024, MaxSize: 2048},
		"avg below min":      {MinSize: 2048, AvgSize: 1024, MaxSize: 4096},
		"max below avg":      {MinSize: 512, AvgSize: 4096, MaxSize: 1024},
		"avg not power of 2": {MinSize: 512, AvgSize: 5000, MaxSize: 16384},
		"bad normalization":  {MinSize: 512, AvgSize: 4096, MaxSize: 16384, Normalization: 9},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate() = nil, want error for %+v", cfg)
			}
			if _, err := New(newByteReader(nil), cfg); err == nil {
				t.Fatal("New() = nil error, want error")
			}
		})
	}

	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig is invalid: %v", err)
	}
}

// Normalization 0 must still produce a working chunker — it reduces to plain
// Gear CDC with a single mask.
func TestNormalizationZero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Normalization = 0

	data := deterministicData(4<<20, 17)
	chunks, err := Split(data, cfg)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	var rebuilt []byte
	for _, c := range chunks {
		rebuilt = append(rebuilt, c.Data...)
	}
	if !bytes.Equal(rebuilt, data) {
		t.Fatal("normalization 0 lost data")
	}
}

// Chunk.Data must not alias the chunker's internal buffer. This is the
// property D-008 exists to protect; a regression here corrupts backups
// silently.
func TestChunkDataDoesNotAliasBuffer(t *testing.T) {
	data := deterministicData(2<<20, 23)
	c, err := New(newByteReader(data), DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first, err := c.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	snapshot := make([]byte, first.Len())
	copy(snapshot, first.Data)

	// Pull several more chunks, which forces buffer refills and shuffles.
	for range 8 {
		if _, err := c.Next(); err != nil {
			break
		}
	}

	if !bytes.Equal(first.Data, snapshot) {
		t.Fatal("first chunk's data changed after subsequent reads: Chunk.Data aliases the internal buffer")
	}
}

func BenchmarkChunker(b *testing.B) {
	data := deterministicData(16<<20, 1)
	cfg := DefaultConfig()

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		c, err := New(newByteReader(data), cfg)
		if err != nil {
			b.Fatal(err)
		}
		for {
			if _, err := c.Next(); err != nil {
				break
			}
		}
	}
}

// Isolates the boundary scan from allocation and copying, so the two costs
// can be reasoned about separately.
func BenchmarkBoundaryScan(b *testing.B) {
	data := deterministicData(1<<20, 2)
	c, err := New(newByteReader(nil), DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		pos := 0
		for pos < len(data) {
			pos += c.boundary(data[pos:])
		}
	}
}
