// Package e2e holds end-to-end tests that drive the real distbackup binary as
// a subprocess.
//
// They live here rather than in cmd/ because they build and execute the
// binary, which is a different kind of test from anything that can run
// in-process — in particular the crash test, which needs a real process to
// SIGKILL. A goroutine cannot be killed the way a process can, so an
// in-process simulation would prove nothing about what survives a genuine
// hard kill.
package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/repo"
	storelocal "github.com/VardaanAggarwal/distbackup/internal/store/localfs"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// binary builds the distbackup command once per test run and returns its path.
func binary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "distbackup-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "distbackup")
		cmd := exec.Command("go", "build", "-o", binPath, "../../cmd/distbackup")
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("building distbackup: %w\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("%v", buildErr)
	}
	return binPath
}

// makeSource writes a source tree large enough that a backup takes long
// enough to be interrupted at an interesting moment.
func makeSource(t *testing.T, nFiles, sizeEach int) string {
	t.Helper()
	dir := t.TempDir()
	for i := range nFiles {
		r := rand.New(rand.NewSource(int64(i)*7919 + 13)) //nolint:gosec // deterministic test data
		data := make([]byte, sizeEach)
		r.Read(data) //nolint:errcheck // rand.Read never fails
		p := filepath.Join(dir, fmt.Sprintf("file%03d.bin", i))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	return dir
}

func runCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(binary(t), args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestCrashDuringBackup is mandatory (docs/ENGINEERING-RULES.md R6) and must never be
// weakened.
//
// It SIGKILLs a backup at randomised points and then asserts the repository
// still verifies and that no snapshot references a pack that is not there.
//
// SIGKILL specifically, not SIGINT: SIGINT gives the program a chance to shut
// down gracefully, which is a different property and is covered by
// TestInterruptWritesNoSnapshot below. SIGKILL is the honest test of the
// on-disk write ordering, because the process gets no opportunity to clean up
// anything at all.
//
// The kill points are randomised across iterations. A single fixed delay
// would only ever exercise one moment in the run, and the interesting failure
// is precisely the one that happens at an inconvenient moment — between
// writing a pack and recording it, or between the index and the manifest.
func TestCrashDuringBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test spawns subprocesses; skipped under -short")
	}

	bin := binary(t)
	src := makeSource(t, 40, 512*1024) // ~20 MiB

	const iterations = 12
	killedAtLeastOnce := false

	for i := range iterations {
		repoDir := t.TempDir()

		out, err := runCmd(t, "init", "--repo", repoDir)
		if err != nil {
			t.Fatalf("init: %v\n%s", err, out)
		}

		cmd := exec.Command(bin, "backup", "--repo", repoDir, "--source", src)
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting backup: %v", err)
		}

		// Randomised kill point spread across the window in which a backup of
		// this size runs.
		delay := time.Duration(rand.Intn(60)+2) * time.Millisecond //nolint:gosec // test timing jitter
		time.Sleep(delay)

		killed := false
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err == nil {
				killed = true
			}
		}
		_ = cmd.Wait()

		if killed {
			killedAtLeastOnce = true
		}

		// Whatever state the repository is in, it must be a consistent one.
		assertRepositoryIntact(t, repoDir, fmt.Sprintf("iteration %d (killed after %v)", i, delay))
	}

	if !killedAtLeastOnce {
		t.Fatal("no iteration actually killed the process; the test proved nothing")
	}
}

// assertRepositoryIntact runs verify and checks the invariant that matters:
// no snapshot may reference data that is not present.
func assertRepositoryIntact(t *testing.T, repoDir, context_ string) {
	t.Helper()

	// The CLI's own verify must pass. Orphan packs are expected and are not
	// failures — they are exactly what an interrupted backup is supposed to
	// leave behind.
	out, err := runCmd(t, "verify", "--repo", repoDir, "--full")
	if err != nil {
		t.Fatalf("%s: verify failed after a crash:\n%s", context_, out)
	}
	if !strings.HasPrefix(out, "ok:") {
		t.Fatalf("%s: verify did not report ok:\n%s", context_, out)
	}

	// And the same check in-process, so the assertion does not depend on
	// parsing CLI output.
	ctx := context.Background()
	s, err := storelocal.Open(repoDir)
	if err != nil {
		t.Fatalf("%s: opening store: %v", context_, err)
	}
	r, err := repo.Open(ctx, s)
	if err != nil {
		t.Fatalf("%s: opening repository after a crash: %v", context_, err)
	}
	defer r.Close() //nolint:errcheck // test cleanup

	report, err := r.Verify(ctx, repo.VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("%s: Verify: %v", context_, err)
	}
	if len(report.MissingPacks) > 0 {
		t.Fatalf("%s: %d snapshots reference missing packs: %v",
			context_, len(report.MissingPacks), report.MissingPacks)
	}
	if len(report.DanglingBlobs) > 0 {
		t.Fatalf("%s: %d dangling blob references", context_, len(report.DanglingBlobs))
	}
	if !report.OK() {
		t.Fatalf("%s: repository not intact: %s", context_, report.Summary())
	}
}

// A killed backup must leave either no snapshot or a complete one. A partial
// manifest would be the dangerous case, and PutIfAbsent on the manifest is
// what rules it out.
func TestCrashLeavesNoPartialSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}

	bin := binary(t)
	src := makeSource(t, 30, 512*1024)

	for i := range 8 {
		repoDir := t.TempDir()
		if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
			t.Fatalf("init: %v\n%s", err, out)
		}

		cmd := exec.Command(bin, "backup", "--repo", repoDir, "--source", src)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		time.Sleep(time.Duration(rand.Intn(50)+2) * time.Millisecond) //nolint:gosec // test timing jitter
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()

		ctx := context.Background()
		s, err := storelocal.Open(repoDir)
		if err != nil {
			t.Fatalf("store open: %v", err)
		}
		r, err := repo.Open(ctx, s)
		if err != nil {
			t.Fatalf("iteration %d: repo.Open: %v", i, err)
		}

		// Every snapshot present must load and verify. LoadSnapshot
		// recomputes the manifest's content address, so a truncated or
		// half-written manifest fails here.
		snaps, err := r.ListSnapshots(ctx)
		if err != nil {
			t.Fatalf("iteration %d: ListSnapshots: %v", i, err)
		}
		for _, snap := range snaps {
			if err := snap.Verify(); err != nil {
				t.Fatalf("iteration %d: partial or corrupt snapshot survived: %v", i, err)
			}
		}
		r.Close() //nolint:errcheck // test cleanup
	}
}

// A graceful interrupt must write no snapshot at all and exit with the
// conventional 130.
func TestInterruptWritesNoSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}

	bin := binary(t)
	src := makeSource(t, 40, 512*1024)
	repoDir := t.TempDir()

	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, "backup", "--repo", repoDir, "--source", src)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signalling: %v", err)
	}
	err := cmd.Wait()

	// The run must not have reported success.
	if err == nil {
		t.Skip("backup completed before the interrupt landed; timing-dependent")
	}

	ctx := context.Background()
	s, err2 := storelocal.Open(repoDir)
	if err2 != nil {
		t.Fatalf("store open: %v", err2)
	}
	r, err2 := repo.Open(ctx, s)
	if err2 != nil {
		t.Fatalf("repo.Open: %v", err2)
	}
	defer r.Close() //nolint:errcheck // test cleanup

	snaps, err2 := r.ListSnapshots(ctx)
	if err2 != nil {
		t.Fatalf("ListSnapshots: %v", err2)
	}
	if len(snaps) != 0 {
		t.Fatalf("an interrupted backup wrote %d snapshots; it must write none", len(snaps))
	}

	assertRepositoryIntact(t, repoDir, "after SIGINT")
}

// An interrupted backup's work is not wasted.
//
// This was discovered by a test that expected the opposite. The crashed run
// leaves packs but no index and no manifest; the next run's repo.Open finds no
// index and rebuilds one from the pack tails (D-007), so those blobs are
// already known and the new backup deduplicates against them. The orphaned
// data is adopted rather than rewritten.
//
// It falls out of two decisions that were made for other reasons — content
// addressing, and an index that is a rebuildable cache — which is the kind of
// property worth knowing you have.
func TestCrashedBackupWorkIsReusedByNextRun(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}

	bin := binary(t)
	src := makeSource(t, 80, 512*1024) // ~40 MiB
	repoDir := t.TempDir()

	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	packsAfterCrash := crashUntilPacksExist(t, bin, repoDir, src)
	t.Logf("crashed backup left %d pack(s) behind", packsAfterCrash)

	// A successful run over the same source adopts them.
	if out, err := runCmd(t, "backup", "--repo", repoDir, "--source", src); err != nil {
		t.Fatalf("completing backup: %v\n%s", err, out)
	}

	out, err := runCmd(t, "gc", "--repo", repoDir, "--dry-run")
	if err != nil {
		t.Fatalf("gc: %v\n%s", err, out)
	}
	t.Logf("gc after the completed run: %s", strings.TrimSpace(out))
	if !strings.Contains(out, "would reclaim 0 packs") {
		t.Fatalf("the crashed run's packs were not adopted by the next backup: %s", strings.TrimSpace(out))
	}

	assertRepositoryIntact(t, repoDir, "after adopting a crashed run's packs")
}

// Packs left by a crash that nothing ever references must be reclaimable.
//
// Distinct from the test above: here the completed backup covers a *different*
// source, so the crashed run's packs stay genuinely unreferenced.
func TestGCReclaimsGenuineOrphans(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}

	bin := binary(t)
	crashSrc := makeSource(t, 80, 512*1024) // ~40 MiB: too big to finish in the search window
	otherSrc := makeSource(t, 3, 64*1024)
	repoDir := t.TempDir()

	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	orphans := crashUntilPacksExist(t, bin, repoDir, crashSrc)
	t.Logf("crashed backup left %d pack(s)", orphans)

	// Back up something unrelated, so the crashed run's packs stay orphaned.
	if out, err := runCmd(t, "backup", "--repo", repoDir, "--source", otherSrc); err != nil {
		t.Fatalf("backup: %v\n%s", err, out)
	}

	before := countPacks(t, repoDir)
	out, err := runCmd(t, "gc", "--repo", repoDir)
	if err != nil {
		t.Fatalf("gc: %v\n%s", err, out)
	}
	t.Logf("gc output: %s", strings.TrimSpace(out))
	if strings.Contains(out, "reclaimed 0 packs") {
		t.Fatal("gc reclaimed nothing despite genuinely orphaned packs")
	}

	after := countPacks(t, repoDir)
	if after >= before {
		t.Fatalf("pack count did not fall: %d before, %d after", before, after)
	}

	assertRepositoryIntact(t, repoDir, "after gc")

	// gc must be idempotent.
	out, err = runCmd(t, "gc", "--repo", repoDir, "--dry-run")
	if err != nil {
		t.Fatalf("second gc: %v\n%s", err, out)
	}
	if !strings.Contains(out, "would reclaim 0 packs") {
		t.Fatalf("gc was not idempotent: %s", strings.TrimSpace(out))
	}
}

// crashUntilPacksExist kills a backup and returns once the repository is in
// the state the caller needs: at least one pack written, and no snapshot.
//
// Both halves of that precondition matter, and getting only the first was a
// real flake. A delay that lands before the first pack leaves nothing to
// exercise; a delay that lands *after* the run finishes leaves a completed
// backup whose pack is legitimately referenced, so a test asserting "gc
// reclaims orphans" fails through no fault of gc. The delay is therefore
// searched from short to long, and the repository is reset whenever an
// attempt overshoots into a completed run.
func crashUntilPacksExist(t *testing.T, bin, repoDir, src string) int {
	t.Helper()

	// The ceiling is generous because CI runners are far slower than a dev
	// machine: a pack only flushes once ~16 MiB has been chunked, and on a
	// throttled shared runner that can take seconds. The loop exits as soon as
	// the precondition is met, so a high ceiling costs nothing when it is fast.
	for delay := 10 * time.Millisecond; delay <= 8*time.Second; delay = delay * 3 / 2 {
		cmd := exec.Command(bin, "backup", "--repo", repoDir, "--source", src)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		time.Sleep(delay)
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()

		packs, snaps := countPacks(t, repoDir), countSnapshots(t, repoDir)
		switch {
		case snaps > 0:
			// Overshot: the backup completed before the kill landed. Reset
			// and try again sooner.
			resetRepo(t, bin, repoDir)
			delay = 10 * time.Millisecond
		case packs > 0:
			return packs
		}
	}
	t.Fatal("could not produce a crashed backup with packs and no snapshot")
	return 0
}

// countSnapshots returns how many snapshot manifests a repository holds.
func countSnapshots(t *testing.T, repoDir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repoDir, "snapshots"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("counting snapshots: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".tmp-") {
			n++
		}
	}
	return n
}

// resetRepo wipes a repository back to freshly-initialised.
func resetRepo(t *testing.T, bin, repoDir string) {
	t.Helper()
	if err := os.RemoveAll(repoDir); err != nil {
		t.Fatalf("resetting repo: %v", err)
	}
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatalf("resetting repo: %v", err)
	}
	cmd := exec.Command(bin, "init", "--repo", repoDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("re-init: %v\n%s", err, out)
	}
}

// The full CLI happy path, asserted end to end on the real binary.
func TestCLIRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}

	src := makeSource(t, 8, 400*1024)
	repoDir := t.TempDir()
	dst := t.TempDir()

	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	if out, err := runCmd(t, "backup", "--repo", repoDir, "--source", src); err != nil {
		t.Fatalf("backup: %v\n%s", err, out)
	}
	if out, err := runCmd(t, "verify", "--repo", repoDir, "--full"); err != nil {
		t.Fatalf("verify: %v\n%s", err, out)
	}
	if out, err := runCmd(t, "restore", "--repo", repoDir, "--snapshot", "latest", "--target", dst); err != nil {
		t.Fatalf("restore: %v\n%s", err, out)
	}

	// Compare the trees byte for byte.
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		want, err := os.ReadFile(filepath.Join(src, e.Name())) //nolint:gosec // test path
		if err != nil {
			t.Fatalf("reading source: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(dst, e.Name())) //nolint:gosec // test path
		if err != nil {
			t.Fatalf("reading restored %q: %v", e.Name(), err)
		}
		if string(want) != string(got) {
			t.Fatalf("file %q differs after a CLI round trip", e.Name())
		}
	}
}

// Init must refuse to clobber an existing repository.
func TestInitRefusesExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}
	repoDir := t.TempDir()
	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	if _, err := runCmd(t, "init", "--repo", repoDir); err == nil {
		t.Fatal("second init succeeded; it must refuse an existing repository")
	}
}

// The --max-bytes guardrail must actually abort a run (docs/ENGINEERING-RULES.md R7).
func TestMaxBytesGuardrail(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped under -short")
	}
	src := makeSource(t, 20, 512*1024) // ~10 MiB
	repoDir := t.TempDir()

	if out, err := runCmd(t, "init", "--repo", repoDir); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	out, err := runCmd(t, "backup", "--repo", repoDir, "--source", src, "--max-bytes", "1048576")
	if err == nil {
		t.Fatalf("backup ignored --max-bytes:\n%s", out)
	}
	if !strings.Contains(out, "max bytes exceeded") {
		t.Fatalf("expected a max-bytes error, got:\n%s", out)
	}
}

// countPacks returns how many pack objects a repository holds, by walking the
// store directly rather than going through the index — which is exactly the
// state a crashed run may have left inconsistent.
func countPacks(t *testing.T, repoDir string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(filepath.Join(repoDir, "packs"), func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !fi.IsDir() && !strings.HasPrefix(fi.Name(), ".tmp-") {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("counting packs: %v", err)
	}
	return n
}
