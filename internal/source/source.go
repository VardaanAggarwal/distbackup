// Package source defines what distbackup can back up.
//
// There are two interfaces, not one, and the split is deliberate
// (docs/DECISIONS.md D-003):
//
//   - A BlockSource is a fixed-size block device — an EBS snapshot, a disk
//     image, a synthetic test device. It has stable, aligned, addressable
//     blocks and usually a native "what changed" query.
//   - A FileSource is a tree of variable-length byte streams with no stable
//     addressing: insert a byte at the front of a file and every subsequent
//     offset moves.
//
// Unifying them would force one side to lie. Either a file path carries a
// synthetic block index, or a block device fakes a directory walk, and the
// pipeline then works around the lie at every call site. Two honest
// interfaces cost one extra entry point and buy a design that reads clearly.
//
// Both interfaces are owned by the core (docs/ENGINEERING-RULES.md R11). Providers satisfy
// them; nothing here knows that AWS or GCP exist.
package source

import (
	"context"
	"io"
	"time"
)

// BlockRef identifies one block within a block source.
type BlockRef struct {
	// Index is the block's index within the device.
	Index int64

	// Token is provider-scoped opaque state needed to fetch this block.
	//
	// It exists because the EBS direct API does not let a caller read a block
	// by index alone: ListSnapshotBlocks returns a block token per block, and
	// GetSnapshotBlock requires it. Modelling that here rather than hiding it
	// inside the provider keeps the provider stateless — it does not have to
	// retain a map of every token it has seen — and makes the expiry
	// behaviour visible to the pipeline, which is what actually has to plan
	// around it.
	//
	// A provider with no such concept (a local file, the synthetic device)
	// leaves it empty. Nothing outside the provider may interpret it.
	Token string

	// Expiry is when Token stops being usable, or the zero time if it does
	// not expire.
	//
	// Verified 2026-08-26: EBS block tokens last 7 days, but the pagination
	// NextToken lasts only 60 minutes — the pagination window is the binding
	// constraint on a long backup, not this one. See docs/RISKS.md R-006.
	Expiry time.Time
}

// Expired reports whether the reference's token is known to have expired.
func (r BlockRef) Expired(now time.Time) bool {
	return !r.Expiry.IsZero() && now.After(r.Expiry)
}

// BlockSource is a device made of fixed-size blocks.
//
// Implementations must be safe for concurrent ReadBlock calls: the pipeline
// reads from a fixed pool of workers.
type BlockSource interface {
	// BlockSize returns the fixed block size in bytes.
	BlockSize() int64

	// Size returns the device's logical size in bytes.
	Size(ctx context.Context) (int64, error)

	// ListBlocks calls fn for every block that has data.
	//
	// Sparse devices report only written blocks — verified for EBS on
	// 2026-08-26: "It returns only block indexes and tokens that have data
	// written to them." So a listing is not size/BlockSize entries, and
	// nothing may assume it is.
	//
	// Returning an error from fn stops the listing and is returned.
	ListBlocks(ctx context.Context, fn func(BlockRef) error) error

	// ListChangedBlocks calls fn for every block that differs from the named
	// prior snapshot. Providers without a native changed-block query return
	// errs.KindUnsupported, and the pipeline falls back to a full listing.
	ListChangedBlocks(ctx context.Context, since string, fn func(ChangedBlockRef) error) error

	// ReadBlock reads one block into buf, returning the number of bytes read.
	// buf must be at least BlockSize bytes.
	ReadBlock(ctx context.Context, ref BlockRef, buf []byte) (int, error)

	// ID returns a stable identifier for what is being read, for the
	// snapshot manifest.
	ID() string

	// Close releases resources.
	Close() error
}

// ChangedBlockRef describes a block that differs between two snapshots.
type ChangedBlockRef struct {
	// Ref locates the block in the newer snapshot.
	Ref BlockRef

	// IsNew reports that the block does not exist in the older snapshot at
	// all, as opposed to existing with different contents.
	//
	// This mirrors the EBS contract exactly. Verified 2026-08-26:
	// ChangedBlock.FirstBlockToken "is absent if the first snapshot does not
	// have the changed block that is on the second snapshot". An absent
	// first token therefore means "newly written", not "unchanged" — reading
	// it the other way would silently skip genuinely new data.
	IsNew bool
}

// FileEntry describes one file found by a FileSource.
type FileEntry struct {
	// Path is relative to the source root, slash-separated.
	Path string
	// Size is the file's length in bytes.
	Size int64
	// Mode is the file's permission bits.
	Mode uint32
	// ModTime is the file's modification time.
	ModTime time.Time
}

// FileSource is a tree of variable-length files.
type FileSource interface {
	// Walk calls fn for every regular file under the root, in a
	// deterministic order.
	//
	// Deterministic order matters for reproducibility: two backups of an
	// unchanged tree should produce identical manifests, and therefore the
	// same snapshot ID. Filesystem readdir order is not stable across
	// platforms, so implementations sort.
	Walk(ctx context.Context, fn func(FileEntry) error) error

	// Open returns the contents of one file. The caller must close it.
	Open(ctx context.Context, path string) (io.ReadCloser, error)

	// Root returns the source root, for the snapshot manifest.
	Root() string

	// Close releases resources.
	Close() error
}
