package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/vardaanaggarwal/distbackup/internal/errs"
	"github.com/vardaanaggarwal/distbackup/internal/repo"
	srclocal "github.com/vardaanaggarwal/distbackup/internal/source/localfs"
	"github.com/vardaanaggarwal/distbackup/internal/store"
	storelocal "github.com/vardaanaggarwal/distbackup/internal/store/localfs"
)

// testTree writes a deterministic directory tree and returns its path.
func testTree(t *testing.T, spec map[string]int) string {
	t.Helper()
	dir := t.TempDir()

	paths := make([]string, 0, len(spec))
	for p := range spec {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		seed := int64(binary.LittleEndian.Uint64(sha256Sum(p)[:8]))
		r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
		data := make([]byte, spec[p])
		r.Read(data) //nolint:errcheck // rand.Read never fails
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	return dir
}

// sha256Sum derives a stable per-path seed so distinct paths get distinct
// contents. Deriving it from len(path) instead, as an earlier version did,
// gave "a.bin" and "b.bin" identical bytes.
func sha256Sum(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

func newRepo(t *testing.T) *repo.Repository {
	t.Helper()
	s, err := storelocal.Open(t.TempDir(), storelocal.WithoutFsync())
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	r, err := repo.Create(context.Background(), s, repo.DefaultConfig())
	if err != nil {
		t.Fatalf("repo.Create: %v", err)
	}
	t.Cleanup(func() { r.Close() }) //nolint:errcheck // test cleanup
	return r
}

// compareTrees asserts two directory trees have identical files and contents.
func compareTrees(t *testing.T, want, got string) {
	t.Helper()

	hashTree := func(root string) map[string]string {
		out := map[string]string{}
		err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(p) //nolint:gosec // test path
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			out[filepath.ToSlash(rel)] = fmt.Sprintf("%x", sum)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
		return out
	}

	a, b := hashTree(want), hashTree(got)
	if len(a) != len(b) {
		t.Fatalf("file counts differ: source has %d, restore has %d", len(a), len(b))
	}
	for path, sum := range a {
		gotSum, ok := b[path]
		if !ok {
			t.Fatalf("restore is missing %q", path)
		}
		if gotSum != sum {
			t.Fatalf("file %q differs: source %s, restore %s", path, sum[:12], gotSum[:12])
		}
	}
}

// TestRoundTrip is mandatory (CLAUDE.md R6) and must never be weakened.
//
// Back up a tree, restore it elsewhere, and compare byte for byte. Every
// other test in this package checks a property; this one checks the thing the
// program is actually for.
func TestRoundTrip(t *testing.T) {
	ctx := context.Background()

	src := testTree(t, map[string]int{
		"empty.bin":            0,
		"tiny.bin":             1,
		"small.txt":            500,
		"under-min.bin":        16*1024 - 1,
		"exactly-min.bin":      16 * 1024,
		"one-chunk.bin":        100 * 1024,
		"many-chunks.bin":      5 << 20,
		"nested/a/b/deep.bin":  300 * 1024,
		"nested/a/sibling.bin": 64 * 1024,
	})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	snap, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("BackupFiles: %v", err)
	}
	if snap.Stats.FilesTotal != 9 {
		t.Fatalf("backed up %d files, want 9", snap.Stats.FilesTotal)
	}

	dst := t.TempDir()
	report, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreFiles: %v", err)
	}
	if report.FilesRestored != 9 {
		t.Fatalf("restored %d files, want 9", report.FilesRestored)
	}

	compareTrees(t, src, dst)

	rep, err := r.Verify(ctx, repo.VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("repository failed verification after round trip: %s", rep.Summary())
	}
}

// Backing up the same tree twice must store nothing the second time. This is
// the property the entire content-addressed design exists to provide.
func TestSecondBackupDeduplicatesEverything(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{
		"a.bin": 2 << 20,
		"b.bin": 1 << 20,
	})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	first, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("first backup: %v", err)
	}
	if first.Stats.BlobsNew == 0 {
		t.Fatal("first backup stored nothing")
	}

	second, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("second backup: %v", err)
	}
	if second.Stats.BlobsNew != 0 {
		t.Fatalf("second backup stored %d new blobs, want 0", second.Stats.BlobsNew)
	}
	if second.Stats.BytesNew != 0 {
		t.Fatalf("second backup wrote %d new bytes, want 0", second.Stats.BytesNew)
	}
	// The total blob count is what must match, not just the new count: the
	// first run can already dedup within itself if two files share content.
	firstTotal := first.Stats.BlobsNew + first.Stats.BlobsDeduped
	if second.Stats.BlobsDeduped != firstTotal {
		t.Fatalf("second backup deduped %d blobs, want %d (the first run saw %d new + %d deduped)",
			second.Stats.BlobsDeduped, firstTotal, first.Stats.BlobsNew, first.Stats.BlobsDeduped)
	}
}

// A one-byte insertion at the front of a large file must re-store only a
// small fraction of it. This is the boundary-shift property from the chunker
// observed end to end, which is what actually matters to a user.
func TestIncrementalBackupAfterInsertion(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	target := filepath.Join(dir, "big.bin")

	rnd := rand.New(rand.NewSource(4242)) //nolint:gosec // deterministic test data
	original := make([]byte, 8<<20)
	rnd.Read(original) //nolint:errcheck // rand.Read never fails
	if err := os.WriteFile(target, original, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r := newRepo(t)
	fs, err := srclocal.Open(dir)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	if _, err := BackupFiles(ctx, r, fs, Options{}); err != nil {
		t.Fatalf("first backup: %v", err)
	}

	modified := append([]byte{0xAB}, original...)
	if err := os.WriteFile(target, modified, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	second, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("second backup: %v", err)
	}

	reStoredFraction := float64(second.Stats.BytesNew) / float64(len(modified))
	t.Logf("after a 1-byte prepend to an 8 MiB file: re-stored %d of %d bytes (%.2f%%), %d new blobs of %d",
		second.Stats.BytesNew, len(modified), reStoredFraction*100,
		second.Stats.BlobsNew, second.Stats.BlobsNew+second.Stats.BlobsDeduped)

	if reStoredFraction > 0.05 {
		t.Fatalf("re-stored %.2f%% of the file after a one-byte insertion; CDC is not resynchronising",
			reStoredFraction*100)
	}
}

// A file whose contents repeat must dedup within a single run.
func TestIntraBackupDeduplication(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(9)) //nolint:gosec // deterministic test data
	block := make([]byte, 1<<20)
	rnd.Read(block) //nolint:errcheck // rand.Read never fails

	// Four identical files.
	for i := range 4 {
		p := filepath.Join(dir, fmt.Sprintf("copy%d.bin", i))
		if err := os.WriteFile(p, block, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	r := newRepo(t)
	fs, err := srclocal.Open(dir)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	snap, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("BackupFiles: %v", err)
	}

	t.Logf("4 identical 1 MiB files: %d new blobs, %d deduped, dedup ratio %.2f",
		snap.Stats.BlobsNew, snap.Stats.BlobsDeduped, snap.Stats.DedupRatio())

	if snap.Stats.BlobsDeduped == 0 {
		t.Fatal("identical files produced no deduplication")
	}
	// Three of the four copies must be entirely deduplicated.
	if ratio := snap.Stats.DedupRatio(); ratio < 0.7 {
		t.Fatalf("dedup ratio %.2f for four identical files, want >= 0.7", ratio)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{"a.bin": 1 << 20, "b.bin": 512 * 1024})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	snap, err := BackupFiles(ctx, r, fs, Options{DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}

	// The report must be real, not empty.
	if snap.Stats.FilesTotal != 2 {
		t.Fatalf("dry run reported %d files, want 2", snap.Stats.FilesTotal)
	}
	if snap.Stats.BlobsNew == 0 {
		t.Fatal("dry run reported no blobs; it must still measure the work")
	}

	// But nothing may have been stored.
	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("dry run wrote %d snapshots", len(snaps))
	}

	report, err := r.Verify(ctx, repo.VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if report.Packs != 0 {
		t.Fatalf("dry run wrote %d packs", report.Packs)
	}
}

func TestMaxBytesAborts(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{"big.bin": 4 << 20})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	_, err = BackupFiles(ctx, r, fs, Options{MaxBytes: 1 << 20})
	if err == nil {
		t.Fatal("BackupFiles ignored MaxBytes")
	}
	if !errors.Is(err, ErrMaxBytesExceeded) {
		t.Fatalf("got %v, want ErrMaxBytesExceeded", err)
	}
}

func TestCancellationStopsBackup(t *testing.T) {
	src := testTree(t, map[string]int{
		"a.bin": 4 << 20, "b.bin": 4 << 20, "c.bin": 4 << 20, "d.bin": 4 << 20,
	})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = BackupFiles(ctx, r, fs, Options{})
	if err == nil {
		t.Fatal("BackupFiles succeeded with a cancelled context")
	}
	if errs.KindOf(err) != errs.KindCanceled {
		t.Fatalf("kind = %s, want canceled", errs.KindOf(err))
	}
}

// A cancelled backup must not leave goroutines running. A leak here would
// accumulate across a long-lived process and is invisible in normal testing.
func TestNoGoroutineLeak(t *testing.T) {
	src := testTree(t, map[string]int{"a.bin": 2 << 20, "b.bin": 2 << 20})
	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	before := runtime.NumGoroutine()

	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, _ = BackupFiles(ctx, r, fs, Options{})
		cancel()
	}

	// Give any stragglers a chance to exit before sampling.
	deadline := time.Now().Add(2 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.Gosched()
		after = runtime.NumGoroutine()
		if after <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines grew from %d to %d after cancelled backups", before, after)
}

func TestRestoreRefusesToOverwriteByDefault(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{"a.txt": 100})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}
	snap, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("BackupFiles: %v", err)
	}

	dst := t.TempDir()
	existing := filepath.Join(dst, "a.txt")
	if err := os.WriteFile(existing, []byte("do not destroy me"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{}); err == nil {
		t.Fatal("restore overwrote an existing file without --overwrite")
	}

	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "do not destroy me" {
		t.Fatal("the existing file was modified")
	}

	if _, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore with Overwrite: %v", err)
	}
	compareTrees(t, src, dst)
}

// A manifest is data and may come from an untrusted repository. A traversal
// path in one must never let a restore write outside the target.
func TestRestoreRejectsTraversalPaths(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "target")

	for _, bad := range []string{"../escape.txt", "a/../../escape.txt", "/etc/passwd", ""} {
		if _, err := safeJoin(dst, bad); err == nil {
			t.Errorf("safeJoin accepted %q", bad)
		}
	}
	if _, err := safeJoin(dst, "ok/file.txt"); err != nil {
		t.Errorf("safeJoin rejected a legitimate path: %v", err)
	}
}

// A restore that fails partway must not leave a file that looks complete.
func TestRestoreRemovesPartialFileOnFailure(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{"a.bin": 2 << 20})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}
	snap, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("BackupFiles: %v", err)
	}

	// Destroy every pack so blob reads fail partway through the file.
	var packKeys []string
	if err := r.Store().List(ctx, repo.PackPrefix, func(info store.ObjectInfo) error {
		packKeys = append(packKeys, info.Key)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(packKeys) == 0 {
		t.Fatal("test setup failed: no packs were written")
	}
	for _, k := range packKeys {
		if err := r.Store().Delete(ctx, k); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	dst := t.TempDir()
	if _, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{}); err == nil {
		t.Fatal("restore succeeded with no packs present")
	}

	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed restore left %d files behind: %v", len(entries), entries)
	}
}

func TestRestoreRejectsBlockSnapshot(t *testing.T) {
	r := newRepo(t)
	snap := &repo.Snapshot{ID: "0123456789abcdef", Kind: repo.KindBlocks, BlockSize: 512}
	_, err := RestoreFiles(context.Background(), r, snap, t.TempDir(), RestoreOptions{})
	if errs.KindOf(err) != errs.KindUnsupported {
		t.Fatalf("kind = %s, want unsupported", errs.KindOf(err))
	}
}

// Two backups of an unchanged tree must produce the same snapshot ID, which
// requires the manifest to be byte-identical — no map iteration order, no
// worker-completion order leaking into the output.
func TestManifestIsDeterministic(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{
		"a.bin": 1 << 20, "b/c.bin": 512 * 1024, "b/d.bin": 300 * 1024, "e.bin": 100,
	})

	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	var firstFiles []repo.FileEntry
	for run := range 3 {
		r := newRepo(t)
		snap, err := BackupFiles(ctx, r, fs, Options{})
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if run == 0 {
			firstFiles = snap.Files
			continue
		}
		if len(snap.Files) != len(firstFiles) {
			t.Fatalf("run %d has %d files, want %d", run, len(snap.Files), len(firstFiles))
		}
		for i := range firstFiles {
			if snap.Files[i].Path != firstFiles[i].Path {
				t.Fatalf("run %d: file %d is %q, want %q", run, i, snap.Files[i].Path, firstFiles[i].Path)
			}
			if len(snap.Files[i].Chunks) != len(firstFiles[i].Chunks) {
				t.Fatalf("run %d: file %q has %d chunks, want %d",
					run, snap.Files[i].Path, len(snap.Files[i].Chunks), len(firstFiles[i].Chunks))
			}
			for j := range firstFiles[i].Chunks {
				if snap.Files[i].Chunks[j] != firstFiles[i].Chunks[j] {
					t.Fatalf("run %d: file %q chunk %d differs", run, snap.Files[i].Path, j)
				}
			}
		}
	}
}

func TestEmptySourceTree(t *testing.T) {
	ctx := context.Background()
	r := newRepo(t)
	fs, err := srclocal.Open(t.TempDir())
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	snap, err := BackupFiles(ctx, r, fs, Options{})
	if err != nil {
		t.Fatalf("BackupFiles on an empty tree: %v", err)
	}
	if snap.Stats.FilesTotal != 0 {
		t.Fatalf("empty tree produced %d files", snap.Stats.FilesTotal)
	}

	dst := t.TempDir()
	if _, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{}); err != nil {
		t.Fatalf("restoring an empty snapshot: %v", err)
	}
}

// Content restored must be byte-identical even when the pack target is tiny,
// which forces many packs and exercises the roll-over path.
func TestSmallPackTargetStillRoundTrips(t *testing.T) {
	ctx := context.Background()
	src := testTree(t, map[string]int{"a.bin": 3 << 20, "b.bin": 2 << 20})

	r := newRepo(t)
	fs, err := srclocal.Open(src)
	if err != nil {
		t.Fatalf("source open: %v", err)
	}

	snap, err := BackupFiles(ctx, r, fs, Options{PackTargetSize: 256 * 1024})
	if err != nil {
		t.Fatalf("BackupFiles: %v", err)
	}
	if snap.Stats.PacksWritten < 5 {
		t.Fatalf("wrote %d packs with a 256 KiB target, expected many more", snap.Stats.PacksWritten)
	}

	dst := t.TempDir()
	if _, err := RestoreFiles(ctx, r, snap, dst, RestoreOptions{}); err != nil {
		t.Fatalf("RestoreFiles: %v", err)
	}
	compareTrees(t, src, dst)
}

func BenchmarkBackupPipeline(b *testing.B) {
	dir := b.TempDir()
	rnd := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic benchmark data
	const size = 32 << 20
	data := make([]byte, size)
	rnd.Read(data) //nolint:errcheck // rand.Read never fails
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		s, err := storelocal.Open(b.TempDir(), storelocal.WithoutFsync())
		if err != nil {
			b.Fatal(err)
		}
		r, err := repo.Create(context.Background(), s, repo.DefaultConfig())
		if err != nil {
			b.Fatal(err)
		}
		fs, err := srclocal.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, err := BackupFiles(context.Background(), r, fs, Options{}); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		r.Close() //nolint:errcheck // benchmark cleanup
		b.StartTimer()
	}
}
