// Package store defines the object store interface that distbackup writes
// backups into, and the errors every implementation must produce.
//
// The interface is owned by the core, not by any provider (CLAUDE.md R11).
// It is deliberately narrow: it contains what the repository layer needs and
// nothing that exists merely because one vendor's SDK offers it. If a method
// here could only be implemented by S3, the abstraction has leaked.
package store

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
)

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	// Key is the object's full key.
	Key string
	// Size is the object's size in bytes.
	Size int64
	// ModTime is when the object was last written. Providers differ in
	// resolution and in whether it is server- or client-assigned, so it is
	// reported for human consumption and never used for ordering or for a
	// correctness decision. See docs/RISKS.md on clock skew.
	ModTime time.Time
}

// ObjectStore is where backup data lands.
//
// Implementations must be safe for concurrent use: the pipeline calls them
// from a fixed pool of upload workers.
type ObjectStore interface {
	// Get returns the whole object. The caller must close the reader.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// GetRange returns n bytes starting at off. A negative n means "to the
	// end". This is the operation that makes the tail-header pack format
	// cheap to recover from (docs/DECISIONS.md D-007).
	GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, error)

	// Put writes an object, overwriting any existing one.
	Put(ctx context.Context, key string, data []byte) error

	// PutIfAbsent writes an object only if the key does not already exist,
	// reporting whether this call created it.
	//
	// The bool-not-error signature is deliberate (docs/DECISIONS.md D-005):
	// for content-addressed data, "already exists" is the *success* case for
	// deduplication, not a failure. S3 signals it as HTTP 412 and every
	// caller would immediately translate that back into "fine, carry on".
	//
	// Implementations must make this atomic: two concurrent callers writing
	// the same key must not both receive created == true, and the object
	// that ends up stored must be complete, never a partial write.
	PutIfAbsent(ctx context.Context, key string, data []byte) (created bool, err error)

	// List calls fn for every object whose key starts with prefix.
	//
	// Iteration order is unspecified — object stores do not agree on it, and
	// depending on it would be a portability bug that only shows up on the
	// second provider. Returning an error from fn stops iteration and is
	// returned to the caller.
	List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error

	// Stat returns metadata for one object.
	Stat(ctx context.Context, key string) (ObjectInfo, error)

	// Delete removes an object. Deleting a key that does not exist is not an
	// error: the caller's intent — that the key be gone — is satisfied
	// either way, and making it an error would force every caller to write
	// the same IsNotFound check.
	Delete(ctx context.Context, key string) error

	// Close releases any resources held by the store.
	Close() error
}

// MaxKeyLength bounds a key. 1024 is S3's documented limit, and adopting the
// tightest constraint among the targeted providers means a repository written
// against one store is always portable to another.
const MaxKeyLength = 1024

// ValidateKey reports whether a key is acceptable to every supported store.
//
// The rules are the intersection of what the providers allow, enforced up
// front so an invalid key fails identically everywhere rather than being
// accepted locally and rejected in the cloud. The path-traversal rules matter
// most for the filesystem backend, where a key like "../../etc/passwd" would
// otherwise escape the repository root.
func ValidateKey(key string) error {
	const op = "store.ValidateKey"

	switch {
	case key == "":
		return errs.E(errs.KindInvalid, op, errors.New("empty key"))
	case len(key) > MaxKeyLength:
		return errs.E(errs.KindInvalid, op, errors.New("key exceeds maximum length"))
	case strings.HasPrefix(key, "/"):
		return errs.E(errs.KindInvalid, op, errors.New("key must be relative"))
	case strings.Contains(key, "\x00"):
		return errs.E(errs.KindInvalid, op, errors.New("key contains a null byte"))
	case strings.Contains(key, "\\"):
		// Rejected on every platform, not just Windows: a key that means one
		// path locally and a different literal key in an object store is a
		// portability bug waiting to happen.
		return errs.E(errs.KindInvalid, op, errors.New("key contains a backslash"))
	}

	for _, seg := range strings.Split(key, "/") {
		if seg == "." || seg == ".." {
			return errs.E(errs.KindInvalid, op, errors.New("key contains a path traversal segment"))
		}
		if seg == "" {
			return errs.E(errs.KindInvalid, op, errors.New("key contains an empty path segment"))
		}
	}
	return nil
}
