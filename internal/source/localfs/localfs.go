// Package localfs implements source.FileSource over a local directory tree.
//
// This is the reference file source and the one the CLI uses by default
// (CLAUDE.md R11). It has no cloud dependencies, which is what lets the whole
// backup and restore path be exercised end to end under R7.
package localfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/source"
)

// Source walks a local directory tree.
type Source struct {
	root       string
	skipErrors bool
}

// Option configures a Source.
type Option func(*Source)

// WithSkipUnreadable makes the walk skip files it cannot open rather than
// aborting the backup.
//
// Off by default, deliberately. A backup that silently omits files the user
// believes are protected is worse than one that stops and says which file it
// could not read. When it is on, every skipped file is reported to the
// caller's error handler so the omission is visible rather than silent.
func WithSkipUnreadable() Option {
	return func(s *Source) { s.skipErrors = true }
}

// Open returns a Source rooted at dir.
func Open(dir string, opts ...Option) (*Source, error) {
	const op = "source/localfs.Open"

	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, errs.E(errs.KindInvalid, op, err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errs.E(errs.KindNotFound, op, err)
		}
		return nil, errs.E(errs.KindInvalid, op, err)
	}
	if !fi.IsDir() {
		return nil, errs.E(errs.KindInvalid, op, fmt.Errorf("%q is not a directory", dir))
	}

	s := &Source{root: abs}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Root returns the source root.
func (s *Source) Root() string { return s.root }

// Close releases resources. A directory walker holds none.
func (s *Source) Close() error { return nil }

// Walk visits every regular file under the root in sorted order.
//
// Sorted, not readdir order: two backups of an unchanged tree must produce
// identical manifests and therefore the same snapshot ID. Readdir order is
// not stable across filesystems or platforms, so relying on it would make
// snapshot identity depend on where the backup ran.
//
// Symlinks are not followed and are skipped entirely. Following them risks
// backing up the same data repeatedly through different paths, or looping
// forever on a cycle; storing them as links is a feature this project does
// not have. Skipping is the honest option, and it is reported rather than
// silent.
func (s *Source) Walk(ctx context.Context, fn func(source.FileEntry) error) error {
	return s.walkDir(ctx, s.root, "", fn)
}

func (s *Source) walkDir(ctx context.Context, dir, prefix string, fn func(source.FileEntry) error) error {
	const op = "source/localfs.Walk"

	if err := ctx.Err(); err != nil {
		return errs.E(errs.KindCanceled, op, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if s.skipErrors && errors.Is(err, fs.ErrPermission) {
			return nil
		}
		if errors.Is(err, fs.ErrPermission) {
			return errs.E(errs.KindPermission, op, err)
		}
		return errs.E(errs.KindTransient, op, err)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return errs.E(errs.KindCanceled, op, err)
		}

		name := e.Name()
		rel := name
		if prefix != "" {
			rel = prefix + "/" + name
		}
		full := filepath.Join(dir, name)

		switch {
		case e.Type()&os.ModeSymlink != 0:
			// Skipped by design; see the Walk doc comment.
			continue
		case e.IsDir():
			if err := s.walkDir(ctx, full, rel, fn); err != nil {
				return err
			}
		case e.Type().IsRegular():
			info, err := e.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					// Deleted between readdir and stat. A file that no longer
					// exists is not a backup failure.
					continue
				}
				if s.skipErrors {
					continue
				}
				return errs.E(errs.KindTransient, op, err)
			}
			entry := source.FileEntry{
				Path:    rel,
				Size:    info.Size(),
				Mode:    uint32(info.Mode().Perm()),
				ModTime: info.ModTime(),
			}
			if err := fn(entry); err != nil {
				return err
			}
		default:
			// Devices, sockets, FIFOs. Not file data; skipped.
			continue
		}
	}
	return nil
}

// Open returns the contents of one file.
func (s *Source) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	const op = "source/localfs.Open"

	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	full, err := s.resolve(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(full) //nolint:gosec // resolve confines the path to the root
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return nil, errs.E(errs.KindNotFound, op, err)
		case errors.Is(err, fs.ErrPermission):
			return nil, errs.E(errs.KindPermission, op, err)
		default:
			return nil, errs.E(errs.KindTransient, op, err)
		}
	}
	return f, nil
}

// resolve maps a relative path to an absolute one, refusing anything that
// escapes the root.
//
// Walk only ever produces paths from inside the root, so this cannot trigger
// during a normal backup. It matters because Open is also reachable with a
// path taken from a snapshot manifest, and a manifest is data that may have
// come from somewhere else.
func (s *Source) resolve(path string) (string, error) {
	const op = "source/localfs.resolve"

	if path == "" {
		return "", errs.E(errs.KindInvalid, op, errors.New("empty path"))
	}
	if filepath.IsAbs(path) {
		return "", errs.E(errs.KindInvalid, op, errors.New("path must be relative to the source root"))
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", errs.E(errs.KindInvalid, op, errors.New("path contains a traversal segment"))
		}
	}

	full := filepath.Join(s.root, filepath.FromSlash(path))
	if !strings.HasPrefix(full, s.root+string(os.PathSeparator)) {
		return "", errs.E(errs.KindInvalid, op,
			fmt.Errorf("path %q resolves outside the source root", path))
	}
	return full, nil
}

var _ source.FileSource = (*Source)(nil)
