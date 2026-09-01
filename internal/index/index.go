// Package index implements the sharded deduplication index: the map from a
// blob's content address to where that blob is stored.
//
// # Why it is sharded
//
// Every chunk produced by the backup pipeline hits this index exactly once,
// from one of NumCPU hashing workers, to answer "have I seen this content
// before?". A single map behind a single mutex would serialise all of them and
// turn the parallel hashing stage into a queue.
//
// Sharding splits the map into independent pieces with independent locks, so
// N workers touching unrelated blobs contend not at all.
//
// # Why the shard key is free
//
// Blob IDs are SHA-256 digests, which are uniformly distributed by
// construction. The first byte is therefore already a perfectly good shard
// selector, and no hash function is needed to derive one — the content address
// *is* the hash. 256 shards, one per possible first byte.
//
// Written from scratch (docs/ENGINEERING-RULES.md R3).
package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

// NumShards is the number of independent shards.
//
// 256 maps exactly onto the first byte of the blob ID, which makes the shard
// selector a single array index with no arithmetic and no hash call. Rejected:
// a smaller power of two such as 64, which would need a mask and buys nothing
// — 256 sync.RWMutex values are a few kilobytes in total.
const NumShards = 256

// Location records where a blob lives.
type Location struct {
	// PackID is the content address of the pack containing the blob.
	PackID blob.ID
	// Offset is the blob's byte offset within that pack.
	Offset int64
	// Length is the blob's length in bytes.
	Length int64
}

// entrySize is the on-disk size of one serialised entry:
// blob ID + pack ID + offset + length.
const entrySize = blob.IDSize + blob.IDSize + 8 + 8

type shard struct {
	mu sync.RWMutex
	m  map[blob.ID]Location
}

// Index maps blob IDs to their storage locations.
//
// All methods are safe for concurrent use.
type Index struct {
	shards [NumShards]shard
}

// New returns an empty Index.
func New() *Index {
	idx := &Index{}
	for i := range idx.shards {
		idx.shards[i].m = make(map[blob.ID]Location)
	}
	return idx
}

// shardFor returns the shard owning an ID.
//
// The first byte of a SHA-256 digest is uniformly distributed, so this
// balances shards without any hashing of its own.
func (idx *Index) shardFor(id blob.ID) *shard {
	return &idx.shards[id[0]]
}

// Insert records a blob's location if it is not already known.
//
// It returns true if this call was the one that inserted, and false if the
// blob was already present — in which case the existing location is left
// untouched, because a blob's content address determines its bytes and any
// copy is as good as any other.
//
// # The property that matters
//
// The check and the insert happen under a single write lock. That is what
// makes "exactly one caller sees true" hold when many goroutines race on the
// same content, and it is the contract the backup pipeline relies on to
// decide which worker actually stores the blob. TestConcurrentDedup asserts
// it directly with 100 goroutines under -race.
//
// Rejected: the read-lock-then-upgrade pattern — take RLock, look up, release,
// take Lock, insert. It looks like an optimisation for the common
// already-present case, and it is wrong: two goroutines can both observe the
// blob as absent in the gap between releasing the read lock and taking the
// write lock, and both would report true. Two workers would then both store
// the same blob, defeating deduplication in exactly the concurrent case the
// system is built for. The bug is also nearly invisible under light load,
// which is what makes it dangerous.
func (idx *Index) Insert(id blob.ID, loc Location) bool {
	s := idx.shardFor(id)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[id]; exists {
		return false
	}
	s.m[id] = loc
	return true
}

// Lookup returns a blob's location.
func (idx *Index) Lookup(id blob.ID) (Location, bool) {
	s := idx.shardFor(id)

	s.mu.RLock()
	defer s.mu.RUnlock()

	loc, ok := s.m[id]
	return loc, ok
}

// Contains reports whether the index knows the blob.
func (idx *Index) Contains(id blob.ID) bool {
	_, ok := idx.Lookup(id)
	return ok
}

// Len returns the number of blobs in the index.
//
// It locks each shard in turn rather than all at once, so the result is a
// sum of counts taken at slightly different instants. Under concurrent
// insertion it is therefore approximate. That is acceptable — Len is used for
// progress reporting and statistics, never for a correctness decision — and
// the alternative, holding all 256 locks simultaneously, would stall every
// worker to produce a number nobody needs to be exact.
func (idx *Index) Len() int {
	total := 0
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		total += len(s.m)
		s.mu.RUnlock()
	}
	return total
}

// Delete removes a blob from the index, reporting whether it was present.
// Used by garbage collection after a pack is rewritten.
func (idx *Index) Delete(id blob.ID) bool {
	s := idx.shardFor(id)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m[id]; !ok {
		return false
	}
	delete(s.m, id)
	return true
}

// ForEach calls fn for every blob in the index.
//
// Each shard is held under a read lock for the duration of its own iteration,
// so fn must not call back into the index — doing so on the same shard would
// deadlock. Callers that need to mutate collect their changes and apply them
// afterwards.
func (idx *Index) ForEach(fn func(blob.ID, Location) error) error {
	for i := range idx.shards {
		s := &idx.shards[i]
		if err := func() error {
			s.mu.RLock()
			defer s.mu.RUnlock()
			for id, loc := range s.m {
				if err := fn(id, loc); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// ShardStats returns the number of entries in each shard.
//
// This exists to make shard balance measurable rather than assumed. If the
// distribution were ever skewed, the claim that SHA-256's first byte is a good
// shard key would be wrong, and lock contention would be silently worse than
// the design predicts.
func (idx *Index) ShardStats() [NumShards]int {
	var stats [NumShards]int
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		stats[i] = len(s.m)
		s.mu.RUnlock()
	}
	return stats
}

// Serialisation format. The index is a cache that can always be rebuilt by
// reading pack headers, so the format is deliberately simple: a small header
// followed by fixed-size records. No JSON — at millions of entries the parse
// cost and the allocation churn would dominate startup.
var indexMagic = [8]byte{'D', 'B', 'I', 'D', 'X', '0', '0', '1'}

const indexHeaderSize = len(indexMagic) + 8 // magic + uint64 entry count

// MaxEntries bounds how many entries a reader will accept from a serialised
// index, guarding against a corrupted count becoming a huge allocation.
//
// 512 million entries is far beyond anything this implementation is claimed
// to handle (see docs/OPEN_QUESTIONS.md Q-003) and still bounds the
// allocation at a survivable size.
const MaxEntries = 512 << 20

// WriteTo serialises the index.
//
// Entries are written shard by shard, and each shard is held under a read lock
// only while its own entries are copied out. The result is therefore not a
// point-in-time snapshot if writers are active. That is safe for the way the
// pipeline uses it — the index is written once, after all writers have
// finished, before the snapshot manifest is committed — and the alternative
// would be to freeze the whole index during a potentially long write.
func (idx *Index) WriteTo(w io.Writer) (int64, error) {
	const op = "index.WriteTo"

	var written int64

	var hdr [indexHeaderSize]byte
	copy(hdr[:], indexMagic[:])
	binary.LittleEndian.PutUint64(hdr[len(indexMagic):], uint64(idx.Len())) //nolint:gosec // a sum of map lengths is non-negative

	n, err := w.Write(hdr[:])
	written += int64(n)
	if err != nil {
		return written, errs.E(errs.KindTransient, op, err)
	}

	buf := make([]byte, entrySize)
	for i := range idx.shards {
		s := &idx.shards[i]
		s.mu.RLock()
		for id, loc := range s.m {
			copy(buf[0:], id[:])
			copy(buf[blob.IDSize:], loc.PackID[:])
			binary.LittleEndian.PutUint64(buf[2*blob.IDSize:], uint64(loc.Offset))   //nolint:gosec // offsets are non-negative
			binary.LittleEndian.PutUint64(buf[2*blob.IDSize+8:], uint64(loc.Length)) //nolint:gosec // lengths are non-negative
			n, err := w.Write(buf)
			written += int64(n)
			if err != nil {
				s.mu.RUnlock()
				return written, errs.E(errs.KindTransient, op, err)
			}
		}
		s.mu.RUnlock()
	}

	return written, nil
}

// ReadFrom deserialises an index written by WriteTo.
func ReadFrom(r io.Reader) (*Index, error) {
	const op = "index.ReadFrom"

	var hdr [indexHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errs.E(errs.KindCorrupt, op, fmt.Errorf("truncated header: %w", err))
		}
		return nil, errs.E(errs.KindTransient, op, err)
	}
	if string(hdr[:len(indexMagic)]) != string(indexMagic[:]) {
		return nil, errs.E(errs.KindCorrupt, op, errors.New("bad index magic"))
	}

	count := binary.LittleEndian.Uint64(hdr[len(indexMagic):])
	if count > MaxEntries {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("entry count %d exceeds the %d limit", count, MaxEntries))
	}

	idx := New()
	buf := make([]byte, entrySize)

	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, errs.E(errs.KindCorrupt, op,
				fmt.Errorf("truncated at entry %d of %d: %w", i, count, err))
		}

		var id, packID blob.ID
		copy(id[:], buf[0:blob.IDSize])
		copy(packID[:], buf[blob.IDSize:2*blob.IDSize])

		offset := int64(binary.LittleEndian.Uint64(buf[2*blob.IDSize:]))   //nolint:gosec // round-trips a non-negative value
		length := int64(binary.LittleEndian.Uint64(buf[2*blob.IDSize+8:])) //nolint:gosec // round-trips a non-negative value
		if offset < 0 || length < 0 {
			return nil, errs.E(errs.KindCorrupt, op,
				fmt.Errorf("entry %d has a negative offset or length", i))
		}

		idx.Insert(id, Location{PackID: packID, Offset: offset, Length: length})
	}

	// Trailing bytes mean the count and the payload disagree, which means the
	// file is not what it claims to be.
	var probe [1]byte
	if _, err := r.Read(probe[:]); err == nil {
		return nil, errs.E(errs.KindCorrupt, op, errors.New("trailing bytes after the declared entry count"))
	}

	return idx, nil
}
