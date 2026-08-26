package repo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/blob"
	"github.com/vardaanaggarwal/distbackup/internal/errs"
)

// SnapshotKind distinguishes the two shapes of source a snapshot can record.
//
// The split mirrors the two source interfaces (docs/DECISIONS.md D-003): a
// file tree is variable-length streams addressed by path, a block device is
// fixed-size blocks addressed by index. One manifest type carries both rather
// than two manifest types, because everything *else* about a snapshot —
// identity, verification, listing, restore orchestration — is identical.
type SnapshotKind string

const (
	// KindFiles is a snapshot of a file tree, chunked with FastCDC.
	KindFiles SnapshotKind = "files"
	// KindBlocks is a snapshot of a fixed-block device.
	KindBlocks SnapshotKind = "blocks"
)

// FileEntry records one file in a KindFiles snapshot.
type FileEntry struct {
	// Path is the file's path relative to the backup root, slash-separated.
	Path string `json:"path"`
	// Size is the file's length in bytes.
	Size int64 `json:"size"`
	// Mode is the file's permission bits.
	Mode uint32 `json:"mode"`
	// ModTime is the file's modification time, restored on extract.
	ModTime time.Time `json:"mod_time"`
	// Chunks lists the blobs that concatenate to the file's contents, in order.
	Chunks []blob.ID `json:"chunks"`
}

// BlockEntry records one block in a KindBlocks snapshot.
type BlockEntry struct {
	// Index is the block's index within the device.
	Index int64 `json:"index"`
	// Blob is the content address of the block's data.
	Blob blob.ID `json:"blob"`
}

// Stats records what a backup run did. Every field is a count of something
// the run actually observed, never an estimate.
type Stats struct {
	// FilesTotal is the number of files visited (KindFiles only).
	FilesTotal int `json:"files_total"`
	// BlocksTotal is the number of blocks visited (KindBlocks only).
	BlocksTotal int `json:"blocks_total"`
	// BytesTotal is the logical size of the source.
	BytesTotal int64 `json:"bytes_total"`
	// BlobsNew is the number of blobs this run actually stored.
	BlobsNew int `json:"blobs_new"`
	// BlobsDeduped is the number of blobs already present, and therefore not
	// stored again. This plus BlobsNew is the total blob count.
	BlobsDeduped int `json:"blobs_deduped"`
	// BytesNew is the number of bytes actually written to the store.
	BytesNew int64 `json:"bytes_new"`
	// BytesDeduped is the number of source bytes covered by blobs that
	// already existed.
	BytesDeduped int64 `json:"bytes_deduped"`
	// PacksWritten is the number of pack objects created.
	PacksWritten int `json:"packs_written"`
	// Duration is how long the run took.
	Duration time.Duration `json:"duration_ns"`
}

// DedupRatio returns the fraction of source bytes that did not need storing.
//
// Returns 0 for an empty source rather than NaN, so callers can format it
// without special-casing.
func (s Stats) DedupRatio() float64 {
	total := s.BytesNew + s.BytesDeduped
	if total == 0 {
		return 0
	}
	return float64(s.BytesDeduped) / float64(total)
}

// Snapshot is the manifest of one backup run.
//
// It is immutable once written. That is why the ID can be its content
// address, and why there is no rewrite path that would need to preserve
// unknown fields from a future version.
type Snapshot struct {
	// ID is the content address of this manifest. See ComputeID.
	ID string `json:"id"`
	// Kind is the source shape.
	Kind SnapshotKind `json:"kind"`
	// CreatedAt is when the run finished, in UTC.
	//
	// Informational. Nothing in the engine orders or expires on it, because a
	// client clock cannot be trusted — two machines backing up to the same
	// repository can disagree by hours. Callers that list snapshots sort by
	// ID for determinism and display this for humans.
	CreatedAt time.Time `json:"created_at"`
	// Source describes what was backed up, for human consumption.
	Source string `json:"source"`
	// Hostname is the machine that produced the snapshot.
	Hostname string `json:"hostname,omitempty"`
	// Parent is the ID of the snapshot this one was taken against, if any.
	Parent string `json:"parent,omitempty"`
	// BlockSize is the device block size for KindBlocks snapshots.
	BlockSize int64 `json:"block_size,omitempty"`
	// Files lists the files, for KindFiles.
	Files []FileEntry `json:"files,omitempty"`
	// Blocks lists the blocks, for KindBlocks.
	Blocks []BlockEntry `json:"blocks,omitempty"`
	// Stats records what the run did.
	Stats Stats `json:"stats"`
}

// SnapshotPrefix is the key prefix under which manifests are stored.
const SnapshotPrefix = "snapshots/"

// Key returns the object key for this snapshot.
func (s *Snapshot) Key() string { return SnapshotPrefix + s.ID }

// ComputeID returns the content address of the manifest.
//
// The ID field is cleared before hashing, which is what makes the scheme
// non-circular: the hash covers every field *except* the one holding the
// hash. Verify recomputes it the same way, so a manifest whose bytes were
// altered after writing fails to verify — the snapshot gets integrity
// checking for free, with no separate checksum to keep in sync.
//
// Rejected: a random UUID or a timestamp. Both would need a separate checksum
// to detect tampering, and neither would make two identical manifests
// collapse to one object.
func (s *Snapshot) ComputeID() (string, error) {
	clone := *s
	clone.ID = ""

	data, err := json.Marshal(&clone)
	if err != nil {
		return "", errs.E(errs.KindInvalid, "repo.Snapshot.ComputeID", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// Verify recomputes the manifest's content address and checks it matches.
func (s *Snapshot) Verify() error {
	const op = "repo.Snapshot.Verify"

	if s.ID == "" {
		return errs.E(errs.KindCorrupt, op, errors.New("snapshot has no ID"))
	}
	want, err := s.ComputeID()
	if err != nil {
		return err
	}
	if want != s.ID {
		return errs.E(errs.KindCorrupt, op,
			fmt.Errorf("snapshot %s does not hash to its contents (computed %s)", s.ID[:12], want[:12]))
	}
	return nil
}

// Validate checks the manifest is internally consistent.
func (s *Snapshot) Validate() error {
	const op = "repo.Snapshot.Validate"

	switch s.Kind {
	case KindFiles:
		if len(s.Blocks) > 0 {
			return errs.E(errs.KindCorrupt, op, errors.New("a files snapshot lists blocks"))
		}
	case KindBlocks:
		if len(s.Files) > 0 {
			return errs.E(errs.KindCorrupt, op, errors.New("a blocks snapshot lists files"))
		}
		if s.BlockSize <= 0 {
			return errs.E(errs.KindCorrupt, op, errors.New("a blocks snapshot has no block size"))
		}
	default:
		return errs.E(errs.KindUnsupported, op, fmt.Errorf("unknown snapshot kind %q", s.Kind))
	}

	for i, f := range s.Files {
		if f.Path == "" {
			return errs.E(errs.KindCorrupt, op, fmt.Errorf("file entry %d has no path", i))
		}
		if f.Size < 0 {
			return errs.E(errs.KindCorrupt, op, fmt.Errorf("file %q has a negative size", f.Path))
		}
	}
	for i, b := range s.Blocks {
		if b.Index < 0 {
			return errs.E(errs.KindCorrupt, op, fmt.Errorf("block entry %d has a negative index", i))
		}
		if b.Blob.IsZero() {
			return errs.E(errs.KindCorrupt, op, fmt.Errorf("block %d has a zero blob ID", b.Index))
		}
	}
	return nil
}

// BlobIDs returns every distinct blob this snapshot references.
//
// Used by verify (to check each one resolves) and by garbage collection (to
// decide what is still reachable).
func (s *Snapshot) BlobIDs() []blob.ID {
	seen := make(map[blob.ID]struct{})
	var out []blob.ID

	add := func(id blob.ID) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	for _, f := range s.Files {
		for _, id := range f.Chunks {
			add(id)
		}
	}
	for _, b := range s.Blocks {
		add(b.Blob)
	}
	return out
}
