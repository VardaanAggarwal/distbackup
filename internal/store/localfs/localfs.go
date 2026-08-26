// Package localfs implements store.ObjectStore on the local filesystem.
//
// This is the reference implementation and distbackup's default backend
// (CLAUDE.md R11). Everything in the engine works end to end against it with
// no cloud packages present, which is what makes the whole system testable
// under the never-touch-real-cloud rule (R7).
//
// It is also the implementation that has to work hardest for crash safety,
// because a filesystem exposes the intermediate states that an object store
// hides behind an atomic PUT.
package localfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/store"
)

// Store is a filesystem-backed object store rooted at a directory.
type Store struct {
	root string
	// fsyncDisabled turns off durability barriers. Tests that write
	// thousands of small objects set it; nothing else may.
	fsyncDisabled bool
}

// Option configures a Store.
type Option func(*Store)

// WithoutFsync disables the fsync barriers on write.
//
// This exists solely so unit tests that create thousands of objects are not
// dominated by disk flushes. It must never be used by the CLI: without the
// barriers, the crash-safety ordering that the repository layer depends on is
// unenforced, and a power loss can leave a snapshot referencing a pack whose
// bytes never reached the disk.
func WithoutFsync() Option {
	return func(s *Store) { s.fsyncDisabled = true }
}

// Open returns a Store rooted at dir, creating the directory if needed.
func Open(dir string, opts ...Option) (*Store, error) {
	const op = "localfs.Open"

	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, errs.E(errs.KindInvalid, op, err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, errs.E(errs.KindTransient, op, err)
	}

	s := &Store{root: abs}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Root returns the store's root directory.
func (s *Store) Root() string { return s.root }

// path maps a key to an absolute filesystem path.
//
// store.ValidateKey has already rejected traversal segments, but this
// re-checks the resolved path against the root anyway. Defence in depth: the
// consequence of getting it wrong is writing outside the repository, and the
// check costs a string comparison.
func (s *Store) path(key string) (string, error) {
	if err := store.ValidateKey(key); err != nil {
		return "", err
	}
	p := filepath.Join(s.root, filepath.FromSlash(key))
	if p != s.root && !strings.HasPrefix(p, s.root+string(os.PathSeparator)) {
		return "", errs.E(errs.KindInvalid, "localfs.path",
			fmt.Errorf("key %q resolves outside the repository root", key))
	}
	return p, nil
}

// classify converts a filesystem error into the engine's error taxonomy.
func classify(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return errs.E(errs.KindNotFound, op, err)
	case errors.Is(err, fs.ErrExist):
		return errs.E(errs.KindAlreadyExists, op, err)
	case errors.Is(err, fs.ErrPermission):
		return errs.E(errs.KindPermission, op, err)
	default:
		// Disk-full, I/O errors, and the like are treated as transient so the
		// retry layer gets a chance. A genuinely permanent failure will
		// simply exhaust the (short) retry budget.
		return errs.E(errs.KindTransient, op, err)
	}
}

// Get returns the whole object.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	const op = "localfs.Get"
	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p) //nolint:gosec // p is validated and confined to the root
	if err != nil {
		return nil, classify(op, err)
	}
	return f, nil
}

// GetRange returns n bytes starting at off, or to the end if n is negative.
func (s *Store) GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	const op = "localfs.GetRange"
	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}
	if off < 0 {
		return nil, errs.E(errs.KindInvalid, op, errors.New("negative offset"))
	}

	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p) //nolint:gosec // p is validated and confined to the root
	if err != nil {
		return nil, classify(op, err)
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		f.Close() //nolint:errcheck,gosec // already failing; the seek error is what matters
		return nil, classify(op, err)
	}
	if n < 0 {
		return f, nil
	}
	// sectionReadCloser keeps the file handle alive for the caller to close;
	// io.LimitReader alone would leak the descriptor.
	return sectionReadCloser{Reader: io.LimitReader(f, n), closer: f}, nil
}

type sectionReadCloser struct {
	io.Reader
	closer io.Closer
}

func (s sectionReadCloser) Close() error { return s.closer.Close() }

// Put writes an object, replacing any existing one.
//
// The sequence is: write a temp file in the destination directory, fsync it,
// close it, rename it over the target, then fsync the directory.
//
// Every step earns its place. Writing to a temp file means a crash never
// leaves a partially-written object visible under the real key. fsync before
// rename means the rename cannot become durable before the data it points at
// — on a crash that would leave a file that exists but contains garbage.
// Rename is atomic within a filesystem, so a reader sees either the old
// object or the new one. The final directory fsync is what makes the rename
// itself durable; without it the file's contents survive a power loss but the
// name may not.
//
// Rejected: writing directly to the destination. It is one syscall shorter
// and it makes every crash during a write produce a corrupt object under a
// live key — which is precisely the failure the mandatory crash test exists
// to rule out.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	const op = "localfs.Put"
	if err := ctx.Err(); err != nil {
		return errs.E(errs.KindCanceled, op, err)
	}

	p, err := s.path(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return classify(op, err)
	}

	tmp, err := s.writeTemp(dir, data)
	if err != nil {
		return classify(op, err)
	}

	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp) //nolint:errcheck // best-effort cleanup on a failing path
		return classify(op, err)
	}
	return classify(op, s.syncDir(dir))
}

// PutIfAbsent writes an object only if the key is free.
//
// It writes a temp file, then uses link(2) to publish it. link fails with
// EEXIST if the target exists, and it is atomic — so the object that appears
// under the key is always complete, and exactly one of several concurrent
// callers can win.
//
// Rejected: open(O_CREAT|O_EXCL) directly at the destination, then write. The
// exclusivity is atomic but the *content* is not: a crash between create and
// the final write leaves a truncated object under a live key, and a
// concurrent reader can observe it half-written. The temp-then-link sequence
// separates "reserve the name" from "the bytes are already complete", which
// is the property that matters.
//
// This mirrors S3's If-None-Match semantics (docs/DECISIONS.md D-005), so
// both backends present the same contract to the repository layer.
func (s *Store) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	const op = "localfs.PutIfAbsent"
	if err := ctx.Err(); err != nil {
		return false, errs.E(errs.KindCanceled, op, err)
	}

	p, err := s.path(key)
	if err != nil {
		return false, err
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, classify(op, err)
	}

	tmp, err := s.writeTemp(dir, data)
	if err != nil {
		return false, classify(op, err)
	}
	defer os.Remove(tmp) //nolint:errcheck // the temp name is dead either way

	if err := os.Link(tmp, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			// Lost the race, or the blob was already stored. For
			// content-addressed data this is success.
			return false, nil
		}
		return false, classify(op, err)
	}
	return true, classify(op, s.syncDir(dir))
}

// writeTemp writes data to a uniquely-named temp file in dir and fsyncs it.
func (s *Store) writeTemp(dir string, data []byte) (string, error) {
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()

	if _, err := f.Write(data); err != nil {
		f.Close()       //nolint:errcheck // the write error is the one to report
		os.Remove(name) //nolint:errcheck // best-effort cleanup
		return "", err
	}
	if !s.fsyncDisabled {
		if err := f.Sync(); err != nil {
			f.Close()       //nolint:errcheck // the sync error is the one to report
			os.Remove(name) //nolint:errcheck // best-effort cleanup
			return "", err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(name) //nolint:errcheck // best-effort cleanup
		return "", err
	}
	return name, nil
}

// syncDir fsyncs a directory, making a rename or link within it durable.
//
// On a POSIX filesystem, fsyncing a file does not make its *name* durable —
// the directory entry is separate metadata. Skipping this is the classic
// mistake that produces a repository which passes every test and loses its
// most recent snapshot on power loss.
func (s *Store) syncDir(dir string) error {
	if s.fsyncDisabled {
		return nil
	}
	d, err := os.Open(dir) //nolint:gosec // dir is derived from a validated key
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck // the Sync error below is the meaningful one
	return d.Sync()
}

// Stat returns metadata for one object.
func (s *Store) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	const op = "localfs.Stat"
	if err := ctx.Err(); err != nil {
		return store.ObjectInfo{}, errs.E(errs.KindCanceled, op, err)
	}

	p, err := s.path(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return store.ObjectInfo{}, classify(op, err)
	}
	if fi.IsDir() {
		return store.ObjectInfo{}, errs.E(errs.KindNotFound, op,
			fmt.Errorf("%q is a directory, not an object", key))
	}
	return store.ObjectInfo{Key: key, Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// List walks every object under prefix.
func (s *Store) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	const op = "localfs.List"

	err := filepath.WalkDir(s.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return errs.E(errs.KindCanceled, op, cerr)
		}
		if d.IsDir() {
			return nil
		}

		rel, rerr := filepath.Rel(s.root, p)
		if rerr != nil {
			return rerr
		}
		key := filepath.ToSlash(rel)

		// Temp files are an implementation detail of the write path and must
		// never appear as objects — a crashed write would otherwise show up
		// as a bogus key in every listing.
		if strings.HasPrefix(filepath.Base(key), ".tmp-") {
			return nil
		}
		if !strings.HasPrefix(key, prefix) {
			return nil
		}

		fi, ierr := d.Info()
		if ierr != nil {
			// A file that vanished between the walk and the stat is a
			// concurrent delete, not an error worth failing the listing over.
			if errors.Is(ierr, fs.ErrNotExist) {
				return nil
			}
			return ierr
		}
		return fn(store.ObjectInfo{Key: key, Size: fi.Size(), ModTime: fi.ModTime()})
	})

	if err != nil {
		if errs.KindOf(err) != errs.KindUnknown {
			return err // already classified, or a caller error from fn
		}
		return classify(op, err)
	}
	return nil
}

// Delete removes an object. Deleting a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	const op = "localfs.Delete"
	if err := ctx.Err(); err != nil {
		return errs.E(errs.KindCanceled, op, err)
	}

	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return classify(op, err)
	}
	return classify(op, s.syncDir(filepath.Dir(p)))
}

// Close releases resources. The filesystem store holds none.
func (s *Store) Close() error { return nil }

// Compile-time proof that Store satisfies the interface. Without this the
// mismatch would only surface at the first call site.
var _ store.ObjectStore = (*Store)(nil)
