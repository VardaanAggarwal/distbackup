// Package repo implements the repository: the layout of a backup store, the
// snapshot manifests, and the write ordering that makes a crash survivable.
//
// # The crash-safety argument
//
// A backup run writes three kinds of object, always in this order:
//
//  1. packs      — the data itself
//  2. index      — a cache mapping blob IDs to pack locations
//  3. snapshot   — the manifest that references the blobs
//
// Each step is durable before the next begins. The ordering is chosen so that
// the only reachable failure state is a harmless one:
//
//   - Crash during step 1: some packs exist that nothing references. Wasted
//     space, reclaimed by gc. The repository is consistent.
//   - Crash during step 2: the index is a cache and can always be rebuilt
//     from pack tails, so a stale or missing index costs time, not data.
//   - Crash during step 3: no snapshot was written, so the run simply did not
//     happen. The packs from step 1 are orphans, as above.
//
// What the ordering makes impossible is a snapshot that references a pack
// which does not exist. That is the one failure that loses data a user
// believes is backed up, and it is why the manifest is written last and
// atomically.
//
// Written from scratch (CLAUDE.md R3).
package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/vardaanaggarwal/distbackup/internal/blob"
	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/index"
	"github.com/vardaanaggarwal/distbackup/internal/pack"
	"github.com/vardaanaggarwal/distbackup/internal/store"
)

const (
	// PackPrefix is the key prefix for pack objects.
	PackPrefix = "packs/"
	// IndexKey is the key holding the serialised index.
	IndexKey = "index/index"

	defaultPackTargetSize = pack.DefaultTargetSize
)

// Repository is an open backup repository.
//
// Safe for concurrent use: the backup pipeline calls PutPack and the index
// from a fixed pool of workers.
type Repository struct {
	store store.ObjectStore
	cfg   Config
	idx   *index.Index

	// mu guards packSizes, which is a read-through cache of pack object
	// sizes. It is not part of the repository format — losing it costs a
	// Stat, not correctness.
	mu        sync.RWMutex
	packSizes map[blob.ID]int64
}

// PackKey returns the object key for a pack.
//
// The two-character fan-out exists for the filesystem backend, where hundreds
// of thousands of entries in one directory degrades badly. Object stores do
// not need it, but sharing the layout means a repository can be copied
// between backends with a plain recursive file copy and still work.
func PackKey(id blob.ID) string {
	return PackPrefix + id.Prefix() + "/" + id.String()
}

// Create initialises a new repository in s.
func Create(ctx context.Context, s store.ObjectStore, cfg Config) (*Repository, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := writeConfig(ctx, s, cfg); err != nil {
		return nil, err
	}
	return &Repository{
		store:     s,
		cfg:       cfg,
		idx:       index.New(),
		packSizes: make(map[blob.ID]int64),
	}, nil
}

// Open loads an existing repository.
//
// A missing or unreadable index is not an error: the index is a cache, and
// Open rebuilds it from pack tails rather than refusing to work. That is the
// whole point of putting the header at the end of the pack (D-007).
func Open(ctx context.Context, s store.ObjectStore) (*Repository, error) {
	cfg, err := readConfig(ctx, s)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		store:     s,
		cfg:       cfg,
		idx:       index.New(),
		packSizes: make(map[blob.ID]int64),
	}

	if err := r.LoadIndex(ctx); err != nil {
		if !errs.IsNotFound(err) && !errs.IsCorrupt(err) {
			return nil, err
		}
		// Corrupt or absent index: rebuild rather than fail. A corrupt cache
		// is a reason to discard the cache, not to refuse the data.
		if err := r.RebuildIndex(ctx); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Config returns the repository's settings.
func (r *Repository) Config() Config { return r.cfg }

// Index returns the deduplication index.
func (r *Repository) Index() *index.Index { return r.idx }

// Store returns the underlying object store.
func (r *Repository) Store() store.ObjectStore { return r.store }

// Close releases resources.
func (r *Repository) Close() error { return r.store.Close() }

// PutPack stores a pack and records its blobs in the index.
//
// The write uses PutIfAbsent, which makes it idempotent: a pack's key is its
// own content address, so a retry after an ambiguous failure writes identical
// bytes to the identical key, and "already exists" is success rather than a
// conflict (docs/DECISIONS.md D-005).
//
// The index is updated only after the pack is durable. Reversing that would
// leave the index pointing at a pack that does not exist — the exact
// dangling-reference failure the write ordering is designed to exclude.
func (r *Repository) PutPack(ctx context.Context, id blob.ID, data []byte, hdr *pack.Header) error {
	const op = "repo.PutPack"

	if hdr == nil {
		return errs.E(errs.KindInvalid, op, errors.New("nil pack header"))
	}

	if _, err := r.store.PutIfAbsent(ctx, PackKey(id), data); err != nil {
		return err
	}

	r.mu.Lock()
	r.packSizes[id] = int64(len(data))
	r.mu.Unlock()

	for _, e := range hdr.Entries {
		r.idx.Insert(e.ID, index.Location{PackID: id, Offset: e.Offset, Length: e.Length})
	}
	return nil
}

// HasBlob reports whether the index already knows a blob.
func (r *Repository) HasBlob(id blob.ID) bool { return r.idx.Contains(id) }

// GetBlob fetches one blob, verified against its content address.
//
// This is the hot path for restore, so it takes the cheap route: the index
// already knows the pack, offset and length, so it is a single ranged read of
// exactly the right bytes. The pack header is never fetched or parsed.
//
// Verification is unconditional. Returning unverified bytes would mean a
// restore could silently produce data that is not what was backed up, which
// is the worst outcome available to a backup system — strictly worse than
// failing loudly.
func (r *Repository) GetBlob(ctx context.Context, id blob.ID) ([]byte, error) {
	const op = "repo.GetBlob"

	loc, ok := r.idx.Lookup(id)
	if !ok {
		return nil, errs.E(errs.KindNotFound, op,
			fmt.Errorf("blob %s is not in the index", id.Short()))
	}

	rc, err := r.store.GetRange(ctx, PackKey(loc.PackID), loc.Offset, loc.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // read path; the read error below is what matters

	data := make([]byte, loc.Length)
	if _, err := io.ReadFull(rc, data); err != nil {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("reading blob %s from pack %s: %w", id.Short(), loc.PackID.Short(), err))
	}
	if !blob.Verify(id, data) {
		return nil, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("blob %s failed verification: stored bytes do not match its content address", id.Short()))
	}
	return data, nil
}

// SaveIndex writes the index to the store.
//
// Plain Put, not PutIfAbsent: the index is a regenerable cache and later runs
// legitimately replace it. The local backend implements Put as
// write-temp-fsync-rename, so a crash mid-write leaves the previous index
// intact rather than a truncated one.
func (r *Repository) SaveIndex(ctx context.Context) error {
	var buf bytes.Buffer
	if _, err := r.idx.WriteTo(&buf); err != nil {
		return err
	}
	return r.store.Put(ctx, IndexKey, buf.Bytes())
}

// LoadIndex reads the serialised index.
func (r *Repository) LoadIndex(ctx context.Context) error {
	rc, err := r.store.Get(ctx, IndexKey)
	if err != nil {
		return err
	}
	defer rc.Close() //nolint:errcheck // read path

	idx, err := index.ReadFrom(rc)
	if err != nil {
		return err
	}
	r.idx = idx
	return nil
}

// RebuildIndex reconstructs the index by reading every pack's tail.
//
// This is the recovery path that makes the index disposable. Each pack costs
// one ranged read of its last 64 KiB — not a full download — because the
// header lives at the end of the file (D-007). A repository whose index was
// lost is therefore recoverable in roughly (number of packs) small requests
// rather than by re-reading all the data.
func (r *Repository) RebuildIndex(ctx context.Context) error {
	const op = "repo.RebuildIndex"

	rebuilt := index.New()

	err := r.store.List(ctx, PackPrefix, func(info store.ObjectInfo) error {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}

		id, err := packIDFromKey(info.Key)
		if err != nil {
			// A key under packs/ that is not a pack name is not something to
			// guess about, but it is also not a reason to abandon the
			// rebuild. Skip it; verify reports it separately.
			return nil //nolint:nilerr // skipping a stray key is deliberate
		}

		hdr, err := r.readPackHeader(ctx, id, info.Size)
		if err != nil {
			return fmt.Errorf("pack %s: %w", id.Short(), err)
		}
		for _, e := range hdr.Entries {
			rebuilt.Insert(e.ID, index.Location{PackID: id, Offset: e.Offset, Length: e.Length})
		}

		r.mu.Lock()
		r.packSizes[id] = info.Size
		r.mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}

	r.idx = rebuilt
	return nil
}

// readPackHeader fetches and parses a pack's trailing header.
//
// It starts with a fixed-size tail guess and, if that was too small, retries
// once with the exact size ParseTail reports it needs. Bounded at one retry:
// ParseTail returns the true requirement, so a second short read would mean
// the pack changed underneath us, which is corruption rather than a wrong
// guess.
func (r *Repository) readPackHeader(ctx context.Context, id blob.ID, size int64) (*pack.Header, error) {
	tailLen := int64(pack.SuggestedTailSize)
	if tailLen > size {
		tailLen = size
	}

	for attempt := range 2 {
		tail, err := r.readTail(ctx, id, size, tailLen)
		if err != nil {
			return nil, err
		}

		hdr, err := pack.ParseTail(tail, size)
		if err == nil {
			return hdr, nil
		}

		var short *pack.ErrShortTail
		if attempt == 0 && errors.As(err, &short) && short.Need <= size {
			tailLen = short.Need
			continue
		}
		return nil, err
	}
	return nil, errs.E(errs.KindCorrupt, "repo.readPackHeader",
		fmt.Errorf("pack %s: tail did not parse after a sized retry", id.Short()))
}

func (r *Repository) readTail(ctx context.Context, id blob.ID, size, tailLen int64) ([]byte, error) {
	rc, err := r.store.GetRange(ctx, PackKey(id), size-tailLen, tailLen)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // read path

	tail, err := io.ReadAll(rc)
	if err != nil {
		return nil, errs.E(errs.KindTransient, "repo.readTail", err)
	}
	return tail, nil
}

// packIDFromKey extracts a pack's content address from its object key.
func packIDFromKey(key string) (blob.ID, error) {
	base := key
	if i := strings.LastIndex(key, "/"); i >= 0 {
		base = key[i+1:]
	}
	return blob.ParseID(base)
}

// SaveSnapshot computes the manifest's ID and writes it.
//
// This is step 3 of the write ordering and must be the last thing a backup
// run does. PutIfAbsent makes it atomic: a reader sees either no snapshot or
// a complete one, never a partial manifest. Because the ID is the manifest's
// content address, re-writing an identical snapshot is a no-op rather than a
// conflict.
func (r *Repository) SaveSnapshot(ctx context.Context, snap *Snapshot) error {
	const op = "repo.SaveSnapshot"

	if err := snap.Validate(); err != nil {
		return err
	}

	id, err := snap.ComputeID()
	if err != nil {
		return err
	}
	snap.ID = id

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return errs.E(errs.KindInvalid, op, err)
	}
	if _, err := r.store.PutIfAbsent(ctx, snap.Key(), data); err != nil {
		return err
	}
	return nil
}

// LoadSnapshot reads and verifies a manifest.
func (r *Repository) LoadSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	const op = "repo.LoadSnapshot"

	rc, err := r.store.Get(ctx, SnapshotPrefix+id)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // read path

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, errs.E(errs.KindTransient, op, err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, errs.E(errs.KindCorrupt, op, fmt.Errorf("decoding snapshot %s: %w", id, err))
	}
	// Verify before Validate: a manifest whose bytes were altered should be
	// reported as tampered rather than as whatever inconsistency the
	// alteration happened to produce.
	if err := snap.Verify(); err != nil {
		return nil, err
	}
	if err := snap.Validate(); err != nil {
		return nil, err
	}
	return &snap, nil
}

// ListSnapshots returns every snapshot, sorted by ID.
//
// Sorted by ID, not by CreatedAt: the ID is a content address and therefore
// stable and comparable, while CreatedAt comes from whichever client wrote
// the snapshot and two clients can disagree by hours. Sorting on an untrusted
// clock would make listing order depend on someone else's NTP configuration.
func (r *Repository) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	var ids []string
	err := r.store.List(ctx, SnapshotPrefix, func(info store.ObjectInfo) error {
		ids = append(ids, strings.TrimPrefix(info.Key, SnapshotPrefix))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)

	snaps := make([]*Snapshot, 0, len(ids))
	for _, id := range ids {
		snap, err := r.LoadSnapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// packSize returns a pack's size, consulting the store if it is not cached.
func (r *Repository) packSize(ctx context.Context, id blob.ID) (int64, error) {
	r.mu.RLock()
	size, ok := r.packSizes[id]
	r.mu.RUnlock()
	if ok {
		return size, nil
	}

	info, err := r.store.Stat(ctx, PackKey(id))
	if err != nil {
		return 0, err
	}

	r.mu.Lock()
	r.packSizes[id] = info.Size
	r.mu.Unlock()
	return info.Size, nil
}
