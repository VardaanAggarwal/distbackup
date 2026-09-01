package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/repo"
)

// RestoreOptions configures a restore.
type RestoreOptions struct {
	// Workers is the number of goroutines restoring files in parallel.
	// Zero means runtime.NumCPU().
	Workers int

	// Overwrite allows writing over files that already exist at the target.
	//
	// Off by default. A restore that silently overwrites is the one operation
	// in this program that can destroy data the user did not ask it to touch,
	// so it requires an explicit choice.
	Overwrite bool

	// Progress, if set, is called as files complete.
	Progress func(RestoreProgress)
}

// RestoreProgress reports how far a restore has got.
type RestoreProgress struct {
	// FilesDone is the number of files fully written.
	FilesDone int
	// FilesTotal is the number of files in the snapshot.
	FilesTotal int
	// BytesWritten is the number of bytes written so far.
	BytesWritten int64
}

// RestoreReport summarises a completed restore.
type RestoreReport struct {
	// FilesRestored is the number of files written.
	FilesRestored int
	// BytesWritten is the total bytes written.
	BytesWritten int64
	// Duration is how long the restore took.
	Duration time.Duration
}

// RestoreFiles writes a snapshot's contents into dst.
//
// Files are restored in parallel; each worker handles whole files, so no two
// workers ever write to the same file and no locking is needed on the write
// path.
func RestoreFiles(ctx context.Context, r *repo.Repository, snap *repo.Snapshot, dst string, opts RestoreOptions) (*RestoreReport, error) {
	const op = "pipeline.RestoreFiles"

	start := time.Now()

	if snap.Kind != repo.KindFiles {
		return nil, errs.E(errs.KindUnsupported, op,
			fmt.Errorf("snapshot %s is of kind %q, not %q", snap.ID[:12], snap.Kind, repo.KindFiles))
	}

	absDst, err := filepath.Abs(dst)
	if err != nil {
		return nil, errs.E(errs.KindInvalid, op, err)
	}
	if err := os.MkdirAll(absDst, 0o755); err != nil {
		return nil, errs.E(errs.KindTransient, op, err)
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	var (
		mu           sync.Mutex
		filesDone    int
		bytesWritten int64
	)

	g, gctx := newGroup(ctx)
	fileCh := make(chan int, fileQueue)

	g.Go(func() error {
		defer close(fileCh)
		for i := range snap.Files {
			if !send(gctx, fileCh, i) {
				return nil
			}
		}
		return nil
	})

	for range workers {
		g.Go(func() error {
			for {
				idx, ok, canceled := recv(gctx, fileCh)
				if canceled || !ok {
					return nil
				}

				n, err := restoreOneFile(gctx, r, snap.Files[idx], absDst, opts)
				if err != nil {
					return err
				}

				mu.Lock()
				filesDone++
				bytesWritten += n
				done, written := filesDone, bytesWritten
				mu.Unlock()

				if opts.Progress != nil && done%16 == 0 {
					opts.Progress(RestoreProgress{
						FilesDone: done, FilesTotal: len(snap.Files), BytesWritten: written,
					})
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errs.E(errs.KindCanceled, op, err)
	}

	return &RestoreReport{
		FilesRestored: filesDone,
		BytesWritten:  bytesWritten,
		Duration:      time.Since(start),
	}, nil
}

// restoreOneFile writes a single file and returns how many bytes it wrote.
func restoreOneFile(ctx context.Context, r *repo.Repository, f repo.FileEntry, dst string, opts RestoreOptions) (int64, error) {
	const op = "pipeline.restoreFile"

	target, err := safeJoin(dst, f.Path)
	if err != nil {
		return 0, err
	}

	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return 0, errs.E(errs.KindTransient, op, err)
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !opts.Overwrite {
		// O_EXCL turns "the file already exists" into a hard failure instead
		// of silent destruction of whatever was there.
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}

	out, err := os.OpenFile(target, flags, os.FileMode(f.Mode)) //nolint:gosec // safeJoin confines target to dst
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return 0, errs.E(errs.KindAlreadyExists, op,
				fmt.Errorf("%q already exists; pass --overwrite to replace it", f.Path))
		}
		return 0, errs.E(errs.KindTransient, op, err)
	}

	var written int64
	writeErr := func() error {
		for _, id := range f.Chunks {
			if err := ctx.Err(); err != nil {
				return errs.E(errs.KindCanceled, op, err)
			}
			// GetBlob verifies every blob against its content address, so a
			// restore either produces exactly the backed-up bytes or fails.
			data, err := r.GetBlob(ctx, id)
			if err != nil {
				return fmt.Errorf("%s: %w", f.Path, err)
			}
			n, err := out.Write(data)
			written += int64(n)
			if err != nil {
				return errs.E(errs.KindTransient, op, err)
			}
		}
		return nil
	}()

	if writeErr != nil {
		out.Close() //nolint:errcheck // already failing
		// Remove the partial file. Leaving it would mean a failed restore
		// produces something that looks like a real file and is not.
		os.Remove(target) //nolint:errcheck // best-effort cleanup
		return 0, writeErr
	}

	if err := out.Close(); err != nil {
		return 0, errs.E(errs.KindTransient, op, err)
	}

	if written != f.Size {
		os.Remove(target) //nolint:errcheck // best-effort cleanup
		return 0, errs.E(errs.KindCorrupt, op,
			fmt.Errorf("%s: restored %d bytes, manifest says %d", f.Path, written, f.Size))
	}

	// Restore mtime last: writing the contents updates it, so it has to come
	// after. Failure here is cosmetic and must not fail the restore — the
	// data is already correct on disk.
	if !f.ModTime.IsZero() {
		_ = os.Chtimes(target, f.ModTime, f.ModTime)
	}

	return written, nil
}

// safeJoin resolves a manifest path against the destination, refusing
// anything that escapes it.
//
// A snapshot manifest is data, and data can come from somewhere untrusted —
// a repository shared between machines, or one someone else wrote. A path
// like "../../.ssh/authorized_keys" in a manifest would otherwise let a
// restore write anywhere the user can write. This is the check that stops it.
func safeJoin(root, path string) (string, error) {
	const op = "pipeline.safeJoin"

	if path == "" {
		return "", errs.E(errs.KindCorrupt, op, errors.New("snapshot contains an empty path"))
	}
	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") {
		return "", errs.E(errs.KindCorrupt, op,
			fmt.Errorf("snapshot contains an absolute path %q", path))
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", errs.E(errs.KindCorrupt, op,
				fmt.Errorf("snapshot contains a traversal path %q", path))
		}
	}

	joined := filepath.Join(root, filepath.FromSlash(path))
	if !strings.HasPrefix(joined, root+string(os.PathSeparator)) {
		return "", errs.E(errs.KindCorrupt, op,
			fmt.Errorf("path %q resolves outside the restore target", path))
	}
	return joined, nil
}
