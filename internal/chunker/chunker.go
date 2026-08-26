// Package chunker implements FastCDC content-defined chunking over a Gear
// rolling hash.
//
// # Why content-defined chunking at all
//
// Fixed-size chunking splits a stream every N bytes. Insert one byte at the
// front and every subsequent boundary shifts by one, so every chunk changes
// and deduplication against the previous backup finds nothing. This is the
// "boundary-shift problem", and it is the reason a backup of a file after a
// small edit can cost as much as the first backup.
//
// Content-defined chunking picks boundaries by looking at the data itself: a
// boundary occurs wherever a rolling hash of the last few dozen bytes hits a
// pattern. Because the decision depends only on nearby content and not on
// absolute position, an insertion perturbs only the chunk that contains it —
// the boundaries after it fall in the same places, at the same content, and
// deduplication resynchronises. TestBoundaryShift asserts this directly.
//
// Written from scratch (CLAUDE.md R3): this is the algorithmic core of the
// project, and importing it would hollow out every interesting question about
// it.
package chunker

import (
	"errors"
	"fmt"
	"io"
	"math/bits"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
)

// Config holds the chunker's size parameters.
//
// Defaults and the reasoning behind them are in docs/DECISIONS.md D-004.
type Config struct {
	// MinSize is the smallest chunk the chunker will emit, except for the
	// final chunk of a stream, which may be shorter.
	MinSize int

	// AvgSize is the target average chunk size. It is where the chunker
	// switches from the strict mask to the loose one.
	AvgSize int

	// MaxSize is a hard cap. On reaching it the chunker cuts unconditionally.
	MaxSize int

	// Normalization is FastCDC's normalization level: the number of bits by
	// which the two masks differ from the target. 0 disables normalized
	// chunking and reduces this to plain Gear-based CDC.
	Normalization int
}

// Default sizes. See docs/DECISIONS.md D-004.
const (
	DefaultMinSize       = 16 * 1024
	DefaultAvgSize       = 64 * 1024
	DefaultMaxSize       = 256 * 1024
	DefaultNormalization = 2
)

// DefaultConfig returns the parameters used by the backup pipeline.
func DefaultConfig() Config {
	return Config{
		MinSize:       DefaultMinSize,
		AvgSize:       DefaultAvgSize,
		MaxSize:       DefaultMaxSize,
		Normalization: DefaultNormalization,
	}
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	switch {
	case c.MinSize <= 0:
		return errs.E(errs.KindInvalid, "chunker.Config", errors.New("MinSize must be positive"))
	case c.AvgSize <= c.MinSize:
		return errs.E(errs.KindInvalid, "chunker.Config", errors.New("AvgSize must exceed MinSize"))
	case c.MaxSize < c.AvgSize:
		return errs.E(errs.KindInvalid, "chunker.Config", errors.New("MaxSize must be at least AvgSize"))
	case c.Normalization < 0 || c.Normalization > 3:
		return errs.E(errs.KindInvalid, "chunker.Config", errors.New("Normalization must be in [0,3]"))
	}
	// The mask width is derived from log2(AvgSize); a non-power-of-two average
	// would silently round, so reject it rather than surprise the caller with
	// a distribution centred somewhere they did not ask for.
	//
	// The conversion is safe: the switch above has already established
	// AvgSize > MinSize > 0.
	if bits.OnesCount64(uint64(c.AvgSize)) != 1 { //nolint:gosec // AvgSize > MinSize > 0 established above
		return errs.E(errs.KindInvalid, "chunker.Config",
			fmt.Errorf("AvgSize must be a power of two, got %d", c.AvgSize))
	}
	return nil
}

// Chunk is one content-defined chunk of the input stream.
type Chunk struct {
	// Offset is the chunk's byte offset within the stream.
	Offset int64
	// Data holds the chunk's bytes. It is freshly allocated and owned by the
	// caller.
	Data []byte
}

// Len returns the chunk length in bytes.
func (c Chunk) Len() int { return len(c.Data) }

// Chunker splits an io.Reader into content-defined chunks.
//
// It is not safe for concurrent use. The pipeline gives each worker its own
// Chunker rather than sharing one behind a mutex — the whole point of the
// worker pool is parallel hashing, and a shared chunker would serialise it.
type Chunker struct {
	cfg Config

	// maskS ("strict", more bits set) applies below AvgSize, making boundaries
	// rarer there. maskL ("loose", fewer bits set) applies above AvgSize,
	// making them likelier.
	//
	// This is FastCDC's normalized chunking, and it is the single most
	// valuable idea in the algorithm. With one mask, boundary positions are
	// geometrically distributed: a large fraction of chunks come out tiny and
	// a long tail comes out at the maximum. Tiny chunks bloat the index;
	// maximum-size chunks dedup poorly. Two masks squeeze the distribution
	// toward the target from both sides.
	maskS uint64
	maskL uint64

	rd  io.Reader
	buf []byte // read buffer; holds unconsumed input
	pos int    // read cursor within buf
	end int    // end of valid data within buf

	offset int64 // stream offset of buf[pos]
	err    error // sticky read error, returned after the buffer drains
}

// New returns a Chunker reading from rd.
func New(rd io.Reader, cfg Config) (*Chunker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Safe conversion: Validate has established AvgSize > MinSize > 0 and
	// that it is a power of two, so TrailingZeros64 yields log2(AvgSize).
	targetBits := bits.TrailingZeros64(uint64(cfg.AvgSize)) //nolint:gosec // Validate established AvgSize > 0

	// The strict mask tests more bits, so a boundary is 2^Normalization times
	// rarer below the target; the loose mask tests fewer, so a boundary is
	// 2^Normalization times likelier above it.
	strictBits := targetBits + cfg.Normalization
	looseBits := targetBits - cfg.Normalization
	if looseBits < 1 {
		looseBits = 1
	}
	if strictBits > maskHi-maskLo+1 {
		strictBits = maskHi - maskLo + 1
	}

	c := &Chunker{
		cfg:   cfg,
		maskS: spreadMask(strictBits),
		maskL: spreadMask(looseBits),
		rd:    rd,
		// One MaxSize of headroom beyond MaxSize means a full-size chunk can
		// always be found without a refill mid-scan, which keeps the scan loop
		// free of buffer-boundary special cases.
		buf: make([]byte, 2*cfg.MaxSize),
	}
	return c, nil
}

// Mask bit positions. See spreadMask.
const (
	maskLo = 16
	maskHi = 63
)

// spreadMask builds a mask with nbits bits set, spaced as evenly as possible
// across bit positions [maskLo, maskHi].
//
// Two choices are encoded here, both worth defending:
//
// Why generated rather than the literal constants from the FastCDC paper:
// those constants are tuned for an 8 KiB average, and this project targets
// 64 KiB (D-004). Rescaling them by hand would produce numbers nobody could
// check. Generating them from the target size means the relationship between
// AvgSize and the mask is visible in code, and the resulting size
// distribution is measured by TestChunkSizeDistribution rather than assumed.
//
// Why bits 16..63 and not 0..63: with the Gear recurrence h = (h<<1) + G[b],
// bit k of the hash is influenced by roughly the last k+1 bytes. The low bits
// therefore depend on only a handful of recent bytes — bit 0 depends on the
// last byte alone — so including them would let a single byte dominate the
// boundary decision and make boundaries fragile. Starting at bit 16 gives
// every mask bit a history of at least 16 bytes, and spreading to bit 63
// gives the widest bit a ~64-byte window, which is the effective window this
// hash can support at all.
func spreadMask(nbits int) uint64 {
	if nbits <= 0 {
		return 0
	}
	span := maskHi - maskLo
	if nbits == 1 {
		return 1 << uint(maskLo)
	}
	var m uint64
	for i := range nbits {
		// pos is bounded by [maskLo, maskHi] = [16, 63] by construction, so
		// the shift can never exceed the width of a uint64.
		pos := maskLo + (i*span)/(nbits-1)
		m |= uint64(1) << pos
	}
	return m
}

// Next returns the next chunk, or io.EOF when the stream is exhausted.
//
// The returned Chunk.Data is freshly allocated on every call and is owned by
// the caller. This is deliberate and costs an allocation per chunk: the
// alternative — returning a window into the internal buffer — is the single
// most likely source of silent data corruption in this design, because the
// pipeline hands chunks to other goroutines through a channel and the buffer
// would be overwritten underneath them. See docs/DECISIONS.md D-008 and
// docs/RISKS.md R-003. SHA-256 over the chunk dominates the cost of the copy
// in any case; BenchmarkChunker measures both together.
func (c *Chunker) Next() (Chunk, error) {
	c.fill()

	avail := c.buf[c.pos:c.end]
	if len(avail) == 0 {
		// A read error surfaces only once the buffer has drained, so a
		// caller receives every chunk that was legitimately read before the
		// failure and *then* the error. A caller that aborts on error
		// therefore never mistakes a truncated stream for a complete one.
		if c.err != nil && !errors.Is(c.err, io.EOF) {
			return Chunk{}, c.err
		}
		return Chunk{}, io.EOF
	}

	n := c.boundary(avail)

	data := make([]byte, n)
	copy(data, avail[:n])

	chunk := Chunk{Offset: c.offset, Data: data}
	c.pos += n
	c.offset += int64(n)
	return chunk, nil
}

// fill tops up the buffer, moving any unconsumed bytes to the front.
//
// It refills whenever fewer than MaxSize bytes remain, so the scan in
// boundary always sees either a full MaxSize window or genuine end-of-stream.
// Without that guarantee the scan would have to handle a boundary search that
// runs off the end of the buffer, which is exactly the kind of special case
// that produces off-by-one bugs in a format that then has to live forever.
//
// fill returns nothing on purpose. A read error is recorded in c.err rather
// than returned, because the buffer may still hold bytes that have not been
// chunked yet and a reader is permitted to return data alongside an error.
// Returning here would silently drop the tail of the stream.
func (c *Chunker) fill() {
	if c.end-c.pos >= c.cfg.MaxSize || c.err != nil {
		return
	}

	if c.pos > 0 {
		copy(c.buf, c.buf[c.pos:c.end])
		c.end -= c.pos
		c.pos = 0
	}

	for c.end < len(c.buf) {
		n, err := c.rd.Read(c.buf[c.end:])
		c.end += n
		if err != nil {
			c.err = err
			return
		}
		if c.end >= c.cfg.MaxSize {
			return
		}
	}
}

// boundary returns the length of the next chunk within data.
//
// The scan is the heart of FastCDC:
//
//   - Bytes before MinSize are skipped entirely, not just rejected. This is
//     "cut-point skipping": since no boundary may be emitted there, hashing
//     those bytes would be wasted work. It costs a little boundary quality —
//     a natural cut inside the skipped region is suppressed, which is part of
//     why TestBoundaryShift asserts 95% rather than 100% — and buys a
//     measurable speedup, because MinSize is a quarter of the average chunk.
//   - Between MinSize and AvgSize the strict mask makes cuts rare.
//   - Between AvgSize and MaxSize the loose mask makes cuts likely.
//   - At MaxSize the chunker cuts regardless, which bounds memory and read
//     amplification at the cost of a boundary that is not content-defined.
//     Those forced cuts are the ones that fail to resynchronise after an
//     insertion.
func (c *Chunker) boundary(data []byte) int {
	n := len(data)

	// A short tail at end-of-stream is emitted whole; there is no more data
	// to find a boundary in.
	if n <= c.cfg.MinSize {
		return n
	}
	if n > c.cfg.MaxSize {
		n = c.cfg.MaxSize
	}

	normal := c.cfg.AvgSize
	if normal > n {
		normal = n
	}

	var h uint64
	i := c.cfg.MinSize

	for ; i < normal; i++ {
		h = (h << 1) + gearTable[data[i]]
		if h&c.maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		h = (h << 1) + gearTable[data[i]]
		if h&c.maskL == 0 {
			return i + 1
		}
	}
	return n
}

// Split is a convenience wrapper that chunks an in-memory buffer.
// It exists for tests and small inputs; the pipeline uses Next directly so
// that a large file never has to be resident in memory.
func Split(data []byte, cfg Config) ([]Chunk, error) {
	c, err := New(newByteReader(data), cfg)
	if err != nil {
		return nil, err
	}
	var out []Chunk
	for {
		chunk, err := c.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, chunk)
	}
}

// byteReader is a minimal io.Reader over a byte slice.
//
// bytes.NewReader would do, but it also implements io.Seeker and io.ReaderAt,
// and a chunker that accidentally depended on either would work in tests and
// fail on a network stream. This reader offers exactly the one method the
// chunker is allowed to use.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader { return &byteReader{data: data} }

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
