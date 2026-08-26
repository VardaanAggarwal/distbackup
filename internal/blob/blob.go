// Package blob defines the content-addressed identifier used throughout distbackup.
//
// Every unit of stored data — a CDC chunk from a file, or a fixed-size block
// from a block device — is a blob, addressed by the SHA-256 of its contents.
// Content addressing is what makes deduplication fall out for free: two blobs
// with the same bytes have the same ID, so storing the second is a no-op.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// IDSize is the length of an ID in bytes.
const IDSize = sha256.Size

// ID is the content address of a blob: the SHA-256 digest of its bytes.
//
// This is a fixed-size array rather than a []byte for three reasons that all
// matter at the scale the index operates at:
//
//   - It is comparable, so it can be a map key directly. A []byte cannot, and
//     would force a string(id) conversion on every lookup — an allocation on
//     the hottest path in the program.
//   - It is copied by value, so a caller cannot retain a reference into memory
//     the index owns and mutate it afterwards.
//   - There is no nil case, so no function needs to decide what a nil ID means.
//
// Rejected: []byte (allocation per lookup, aliasing hazard) and string
// (immutable and comparable, but hex-or-raw ambiguity invites bugs at the
// boundary where IDs are printed).
type ID [IDSize]byte

// Zero is the zero ID. It is never a valid content address in practice — it
// would require a SHA-256 collision with the all-zero digest — so it doubles
// as a sentinel for "unset".
var Zero ID

// Compute returns the content address of data.
func Compute(data []byte) ID {
	return ID(sha256.Sum256(data))
}

// IsZero reports whether the ID is unset.
func (id ID) IsZero() bool { return id == Zero }

// String returns the full hex encoding of the ID.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the first 12 hex characters, for logs and human-facing output.
//
// 12 hex characters is 48 bits. That is not enough to be unique across a large
// repository and is never used for lookup — only for display, where a full
// 64-character digest makes log lines unreadable.
func (id ID) Short() string {
	return hex.EncodeToString(id[:6])
}

// Prefix returns the first two hex characters, used as the directory fan-out
// component of a pack's storage key.
//
// Two characters gives 256 buckets. This exists for the local filesystem
// backend, where hundreds of thousands of entries in one directory degrades
// badly on some filesystems. Object stores do not need it — S3 has no real
// directories — but a shared layout keeps the two backends byte-identical,
// which means a repository can be copied between them with plain file copy.
func (id ID) Prefix() string {
	return hex.EncodeToString(id[:1])
}

// MarshalText implements encoding.TextMarshaler so IDs serialise as hex
// strings in JSON rather than as base64-encoded byte arrays.
func (id ID) MarshalText() ([]byte, error) {
	buf := make([]byte, hex.EncodedLen(IDSize))
	hex.Encode(buf, id[:])
	return buf, nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *ID) UnmarshalText(text []byte) error {
	if len(text) != hex.EncodedLen(IDSize) {
		return fmt.Errorf("blob: invalid ID length %d, want %d", len(text), hex.EncodedLen(IDSize))
	}
	if _, err := hex.Decode(id[:], text); err != nil {
		return fmt.Errorf("blob: invalid ID encoding: %w", err)
	}
	return nil
}

// ErrMalformedID is returned by ParseID when the input is not a valid hex digest.
var ErrMalformedID = errors.New("blob: malformed ID")

// ParseID decodes a full hex digest into an ID.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != hex.EncodedLen(IDSize) {
		return Zero, fmt.Errorf("%w: length %d, want %d", ErrMalformedID, len(s), hex.EncodedLen(IDSize))
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		// Two %w verbs: callers may match either the sentinel or the
		// underlying hex error. Supported since Go 1.20.
		return Zero, fmt.Errorf("%w: %w", ErrMalformedID, err)
	}
	return id, nil
}

// Verify reports whether data hashes to id. It is the integrity check that
// every read path runs before returning data to a caller.
//
// This is cheap insurance against the failure mode that matters most in a
// backup system: silently returning corrupt data that the user believes is
// their file. A backup that restores wrong bytes is worse than one that
// fails loudly.
func Verify(id ID, data []byte) bool {
	return Compute(data) == id
}
