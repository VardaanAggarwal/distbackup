// Package pack implements distbackup's pack file format.
//
// # Why pack files exist
//
// Chunks average ~72 KiB (see docs/phase-1-summary.md). Storing each as its
// own object would mean one PUT per chunk, and object stores charge per
// request and have per-request latency floors that dominate at that size. A
// 1 GiB backup would be ~14,000 round trips. Packing many blobs into one
// larger object collapses that to a few dozen.
//
// The cost is that reading one blob back means reading part of a larger
// object, which is why the header carries byte offsets and why the format is
// designed for ranged reads.
//
// # Format (version 1)
//
//	[blob 0][blob 1]...[blob n][header JSON][uint32 header length][magic]
//
// The header sits at the END of the file. See docs/DECISIONS.md D-007: this
// permits a single streaming forward pass with bounded memory, and allows a
// pack's contents to be recovered with one small ranged GET of the tail when
// the index is unavailable. The cost is that a reader must seek to the end
// before it can read anything.
//
// Written from scratch (docs/ENGINEERING-RULES.md R3).
package pack

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"crypto/sha256"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

// Magic identifies a distbackup pack file and pins the format version.
//
// The version lives in the magic rather than only in the JSON header so that
// a reader can reject an incompatible file after reading 8 bytes, before
// trusting any length field enough to allocate against it.
var Magic = [8]byte{'D', 'B', 'P', 'A', 'C', 'K', '0', '1'}

// FormatVersion is the pack format version carried in the header.
const FormatVersion = 1

// TrailerSize is the fixed-size footer: uint32 header length plus the magic.
const TrailerSize = 4 + len(Magic)

// MaxHeaderSize bounds how large a header a reader will allocate for.
//
// This is the guard against a corrupted or hostile length field turning into
// a multi-gigabyte allocation. At ~90 bytes per entry, 32 MiB allows roughly
// 370,000 blobs in one pack, far more than DefaultTargetSize permits.
const MaxHeaderSize = 32 << 20

// DefaultTargetSize is the size at which the writer reports itself full.
//
// 16 MiB balances two costs. Larger packs mean fewer requests but more
// wasted transfer when only one blob is needed and more data to rewrite
// during garbage collection. Smaller packs mean more requests. 16 MiB is
// roughly 220 average chunks, which keeps per-request overhead negligible
// while still being small enough that fetching a whole pack to read one blob
// is not catastrophic.
const DefaultTargetSize = 16 << 20

// Entry locates one blob within a pack.
type Entry struct {
	// ID is the blob's content address.
	ID blob.ID `json:"id"`
	// Offset is the blob's byte offset from the start of the pack.
	Offset int64 `json:"offset"`
	// Length is the blob's length in bytes.
	Length int64 `json:"length"`
}

// Header is the pack's trailing index of its own contents.
type Header struct {
	// Version is the pack format version.
	Version int `json:"version"`
	// Entries locate every blob in the pack, in write order.
	Entries []Entry `json:"entries"`
}

// Writer builds a pack file by appending blobs, then writing the header.
//
// It is not safe for concurrent use. The pipeline assigns one Writer per
// pack-assembly worker rather than sharing one behind a mutex; see
// docs/PLAN.md section 5.
type Writer struct {
	w       io.Writer
	hasher  hash.Hash
	entries []Entry
	seen    map[blob.ID]struct{}
	offset  int64
	done    bool
}

// NewWriter returns a Writer that streams a pack into w.
//
// The caller chooses where w points. The pipeline writes into a bytes.Buffer
// because the pack's storage key is the hash of its own bytes, which is not
// known until the last byte is written — so the whole pack must be held
// somewhere addressable before it can be uploaded. DefaultTargetSize bounds
// that at 16 MiB.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:      w,
		hasher: sha256.New(),
		seen:   make(map[blob.ID]struct{}),
	}
}

// Add appends a blob to the pack.
//
// Adding an ID that is already in this pack is a no-op that reports false.
// Deduplicating within a pack matters because the index is consulted before
// a blob reaches the pack writer, but two identical chunks can be in flight
// concurrently and both miss the index — so the writer is the last place the
// duplicate can be caught before it costs storage.
func (p *Writer) Add(id blob.ID, data []byte) (added bool, err error) {
	if p.done {
		return false, errs.E(errs.KindInvalid, "pack.Add", errors.New("writer already finished"))
	}
	if id.IsZero() {
		return false, errs.E(errs.KindInvalid, "pack.Add", errors.New("zero blob ID"))
	}
	// Verify rather than trust. A caller that computed the ID from different
	// bytes than it is storing would produce a pack that fails verification
	// much later, at restore time, with no way to trace it back here.
	if !blob.Verify(id, data) {
		return false, errs.E(errs.KindCorrupt, "pack.Add",
			fmt.Errorf("blob %s does not hash to its contents", id.Short()))
	}
	if _, dup := p.seen[id]; dup {
		return false, nil
	}

	if _, err := p.write(data); err != nil {
		return false, err
	}

	p.entries = append(p.entries, Entry{ID: id, Offset: p.offset, Length: int64(len(data))})
	p.seen[id] = struct{}{}
	p.offset += int64(len(data))
	return true, nil
}

// write sends bytes to both the output and the running pack hash.
func (p *Writer) write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if err != nil {
		return n, errs.E(errs.KindTransient, "pack.write", err)
	}
	// hash.Hash is documented never to return an error.
	p.hasher.Write(b[:n]) //nolint:errcheck // hash.Hash.Write never errors
	return n, nil
}

// Size returns the number of blob bytes written so far, excluding the header.
func (p *Writer) Size() int64 { return p.offset }

// Count returns the number of distinct blobs in the pack.
func (p *Writer) Count() int { return len(p.entries) }

// IsFull reports whether the pack has reached target size and should be
// closed off.
func (p *Writer) IsFull(target int64) bool { return p.offset >= target }

// Contains reports whether the blob is already in this pack.
func (p *Writer) Contains(id blob.ID) bool {
	_, ok := p.seen[id]
	return ok
}

// Finish writes the header and trailer and returns the pack's content address.
//
// The returned ID is the SHA-256 of the entire pack, header and trailer
// included. Making the pack itself content-addressed means an upload is
// idempotent — retrying it writes the identical object under the identical
// key — and it gives `verify` a way to detect a pack whose bytes changed
// underneath the repository.
func (p *Writer) Finish() (blob.ID, *Header, error) {
	if p.done {
		return blob.Zero, nil, errs.E(errs.KindInvalid, "pack.Finish", errors.New("writer already finished"))
	}
	if len(p.entries) == 0 {
		return blob.Zero, nil, errs.E(errs.KindInvalid, "pack.Finish", errors.New("refusing to write an empty pack"))
	}

	hdr := &Header{Version: FormatVersion, Entries: p.entries}
	encoded, err := json.Marshal(hdr)
	if err != nil {
		return blob.Zero, nil, errs.E(errs.KindInvalid, "pack.Finish", err)
	}
	if len(encoded) > MaxHeaderSize {
		return blob.Zero, nil, errs.E(errs.KindInvalid, "pack.Finish",
			fmt.Errorf("header is %d bytes, above the %d limit", len(encoded), MaxHeaderSize))
	}

	if _, err := p.write(encoded); err != nil {
		return blob.Zero, nil, err
	}

	var trailer [TrailerSize]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(encoded))) //nolint:gosec // bounded by MaxHeaderSize above
	copy(trailer[4:], Magic[:])
	if _, err := p.write(trailer[:]); err != nil {
		return blob.Zero, nil, err
	}

	p.done = true

	var id blob.ID
	copy(id[:], p.hasher.Sum(nil))
	return id, hdr, nil
}

// ErrShortTail reports that a tail read did not include the whole header.
// It carries the number of trailing bytes actually required.
type ErrShortTail struct {
	// Need is the number of bytes from the end of the pack that must be read
	// to cover the header and trailer.
	Need int64
}

func (e *ErrShortTail) Error() string {
	return fmt.Sprintf("pack: tail read too short, need the last %d bytes", e.Need)
}

// ParseTail recovers a pack's header from the trailing bytes of the file.
//
// This is the index-free recovery path, and the reason D-007 puts the header
// at the end. Given a pack of known size, a caller fetches some trailing
// window — one ranged GET — and calls this. If the window did not reach far
// enough back, the returned *ErrShortTail says exactly how many bytes are
// needed, so the second attempt is guaranteed to succeed. In practice the
// first guess almost always suffices.
//
// packSize is the total size of the pack; tail must be its last len(tail)
// bytes.
func ParseTail(tail []byte, packSize int64) (*Header, error) {
	const op = "pack.ParseTail"

	if int64(len(tail)) > packSize {
		return nil, errs.E(errs.KindInvalid, op,
			fmt.Errorf("tail of %d bytes exceeds pack size %d", len(tail), packSize))
	}
	if len(tail) < TrailerSize {
		return nil, &ErrShortTail{Need: int64(TrailerSize)}
	}

	trailer := tail[len(tail)-TrailerSize:]
	if !bytes.Equal(trailer[4:], Magic[:]) {
		// Checked before the length field is trusted: a wrong magic means
		// this is not a pack at all, and its "length" is meaningless.
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("bad magic %q, want %q", trailer[4:], Magic[:]))
	}

	headerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if headerLen == 0 {
		return nil, errs.E(errs.KindCorrupt, op, errors.New("header length is zero"))
	}
	if headerLen > MaxHeaderSize {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("header length %d exceeds the %d limit", headerLen, MaxHeaderSize))
	}
	if headerLen+int64(TrailerSize) > packSize {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("header length %d does not fit in a pack of %d bytes", headerLen, packSize))
	}

	need := headerLen + int64(TrailerSize)
	if int64(len(tail)) < need {
		return nil, &ErrShortTail{Need: need}
	}

	start := int64(len(tail)) - need
	encoded := tail[start : int64(len(tail))-int64(TrailerSize)]

	var hdr Header
	if err := json.Unmarshal(encoded, &hdr); err != nil {
		return nil, errs.E(errs.KindCorrupt, op, fmt.Errorf("decoding header: %w", err))
	}
	if hdr.Version != FormatVersion {
		// Refuse rather than guess. A reader that tries to interpret an
		// unknown version is how a format becomes un-evolvable.
		return nil, errs.E(errs.KindUnsupported, op,
			fmt.Errorf("pack format version %d, this build understands %d", hdr.Version, FormatVersion))
	}
	if len(hdr.Entries) == 0 {
		return nil, errs.E(errs.KindCorrupt, op, errors.New("header lists no entries"))
	}

	dataEnd := packSize - need
	for i, e := range hdr.Entries {
		if e.Offset < 0 || e.Length < 0 {
			return nil, errs.E(errs.KindCorrupt, op,
				fmt.Errorf("entry %d has negative offset or length", i))
		}
		if e.Offset+e.Length > dataEnd {
			return nil, errs.E(errs.KindCorrupt, op,
				fmt.Errorf("entry %d (%s) spans [%d,%d), past the end of blob data at %d",
					i, e.ID.Short(), e.Offset, e.Offset+e.Length, dataEnd))
		}
	}

	return &hdr, nil
}

// SuggestedTailSize is a reasonable first ranged-GET size for ParseTail.
//
// It covers a header for roughly 700 blobs, which is more than a
// DefaultTargetSize pack holds at the measured average chunk size, so a
// second round trip is rare in practice rather than merely possible.
const SuggestedTailSize = 64 << 10

// Reader reads blobs out of a pack via random access.
type Reader struct {
	ra     io.ReaderAt
	size   int64
	header *Header
	byID   map[blob.ID]Entry
}

// NewReader parses a pack's header and returns a Reader over it.
func NewReader(ra io.ReaderAt, size int64) (*Reader, error) {
	const op = "pack.NewReader"

	if size < int64(TrailerSize) {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("pack is %d bytes, smaller than the %d-byte trailer", size, TrailerSize))
	}

	tailLen := int64(SuggestedTailSize)
	if tailLen > size {
		tailLen = size
	}

	hdr, err := readTailAt(ra, size, tailLen)
	if err != nil {
		// One retry with the exact size the first attempt asked for. Bounded
		// at one: ParseTail returns the true requirement, so a second short
		// tail would mean the pack changed underneath us.
		var short *ErrShortTail
		if errors.As(err, &short) {
			if short.Need > size {
				return nil, errs.E(errs.KindCorrupt, op,
					fmt.Errorf("header claims to need %d bytes from a %d-byte pack", short.Need, size))
			}
			hdr, err = readTailAt(ra, size, short.Need)
		}
		if err != nil {
			return nil, err
		}
	}

	byID := make(map[blob.ID]Entry, len(hdr.Entries))
	for _, e := range hdr.Entries {
		byID[e.ID] = e
	}

	return &Reader{ra: ra, size: size, header: hdr, byID: byID}, nil
}

func readTailAt(ra io.ReaderAt, size, tailLen int64) (*Header, error) {
	buf := make([]byte, tailLen)
	if _, err := ra.ReadAt(buf, size-tailLen); err != nil && !errors.Is(err, io.EOF) {
		return nil, errs.E(errs.KindTransient, "pack.readTail", err)
	}
	return ParseTail(buf, size)
}

// Header returns the pack's header.
func (r *Reader) Header() *Header { return r.header }

// Entries returns the pack's entries in write order.
func (r *Reader) Entries() []Entry { return r.header.Entries }

// Lookup returns the entry for a blob, if the pack contains it.
func (r *Reader) Lookup(id blob.ID) (Entry, bool) {
	e, ok := r.byID[id]
	return e, ok
}

// ReadBlob returns the contents of one blob, verified against its ID.
//
// Verification is not optional and not configurable. The failure this guards
// against — silently returning corrupt bytes that the user believes are their
// data — is the worst thing a backup system can do, and it is strictly worse
// than failing loudly.
func (r *Reader) ReadBlob(id blob.ID) ([]byte, error) {
	const op = "pack.ReadBlob"

	e, ok := r.byID[id]
	if !ok {
		return nil, errs.E(errs.KindNotFound, op, fmt.Errorf("blob %s not in this pack", id.Short()))
	}

	data := make([]byte, e.Length)
	if _, err := r.ra.ReadAt(data, e.Offset); err != nil {
		return nil, errs.E(errs.KindTransient, op, err)
	}
	if !blob.Verify(id, data) {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("blob %s failed verification: stored bytes do not match its content address", id.Short()))
	}
	return data, nil
}

// VerifyAll reads and verifies every blob in the pack, returning the number
// checked. It is the per-pack workhorse of the `verify` command.
func (r *Reader) VerifyAll() (int, error) {
	for _, e := range r.header.Entries {
		if _, err := r.ReadBlob(e.ID); err != nil {
			return 0, err
		}
	}
	return len(r.header.Entries), nil
}
