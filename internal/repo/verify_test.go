package repo

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

// saveSnapshotFor stores a snapshot referencing the given blobs.
func saveSnapshotFor(t *testing.T, r *Repository, source string, ids []blob.ID) *Snapshot {
	t.Helper()
	snap := &Snapshot{
		Kind:      KindFiles,
		CreatedAt: time.Now().UTC(),
		Source:    source,
		Files:     []FileEntry{{Path: "data", Size: 1, Chunks: ids}},
	}
	if err := r.SaveSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return snap
}

func TestVerifyCleanRepository(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, ids := storePack(t, r, []byte("alpha"), []byte("beta"))
	saveSnapshotFor(t, r, "/src", ids)

	report, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("clean repository failed verification: %s", report.Summary())
	}
	if report.Snapshots != 1 || report.Packs != 1 || report.BlobsReferenced != 2 {
		t.Fatalf("unexpected report: %s", report.Summary())
	}
	if report.BlobsRead != 0 {
		t.Fatalf("structural verify read %d blobs, want 0", report.BlobsRead)
	}
}

func TestVerifyFullReadsEveryBlob(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, ids := storePack(t, r, []byte("alpha"), []byte("beta"), []byte("gamma"))
	saveSnapshotFor(t, r, "/src", ids)

	report, err := r.Verify(ctx, VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("clean repository failed full verification: %s", report.Summary())
	}
	if report.BlobsRead != 3 {
		t.Fatalf("full verify read %d blobs, want 3", report.BlobsRead)
	}
}

// A snapshot referencing a pack that is gone is the failure mode the write
// ordering exists to prevent. Verify must detect it if it ever happens.
func TestVerifyDetectsMissingPack(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	packID, ids := storePack(t, r, []byte("alpha"), []byte("beta"))
	saveSnapshotFor(t, r, "/src", ids)

	if err := r.Store().Delete(ctx, PackKey(packID)); err != nil {
		t.Fatalf("Delete pack: %v", err)
	}

	report, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if report.OK() {
		t.Fatal("Verify reported OK despite a missing pack")
	}
	if len(report.MissingPacks) != 1 || report.MissingPacks[0] != packID {
		t.Fatalf("MissingPacks = %v, want [%s]", report.MissingPacks, packID.Short())
	}
}

// Full verification must catch corrupted bytes that structural verification
// cannot see.
func TestVerifyFullDetectsCorruption(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	packID, ids := storePack(t, r, bytes.Repeat([]byte("data"), 500))
	saveSnapshotFor(t, r, "/src", ids)

	full, err := readWholeObject(ctx, r.Store(), PackKey(packID))
	if err != nil {
		t.Fatalf("read pack: %v", err)
	}
	full[20] ^= 0xFF
	if err := r.Store().Put(ctx, PackKey(packID), full); err != nil {
		t.Fatalf("Put corrupted pack: %v", err)
	}

	// Structural verification cannot see byte-level corruption.
	structural, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !structural.OK() {
		t.Fatal("structural verify flagged corruption it cannot actually detect")
	}

	// Full verification must.
	deep, err := r.Verify(ctx, VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("Verify full: %v", err)
	}
	if deep.OK() {
		t.Fatal("full verify missed a corrupted blob")
	}
}

// Orphan packs are the residue of an interrupted backup. They must be
// reported but must not make the repository "broken" — otherwise every
// crashed run would leave it permanently failing.
func TestVerifyReportsOrphansWithoutFailing(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, referenced := storePack(t, r, []byte("referenced"))
	orphanID, _ := storePack(t, r, []byte("orphaned"))
	saveSnapshotFor(t, r, "/src", referenced)

	report, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("orphan packs made the repository fail verification: %s", report.Summary())
	}
	if len(report.OrphanPacks) != 1 || report.OrphanPacks[0] != orphanID {
		t.Fatalf("OrphanPacks = %v, want [%s]", report.OrphanPacks, orphanID.Short())
	}
}

func TestVerifyEmptyRepository(t *testing.T) {
	r := newTestRepo(t)
	report, err := r.Verify(context.Background(), VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("empty repository failed verification: %s", report.Summary())
	}
}

func TestCollectGarbageRemovesOrphans(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, referenced := storePack(t, r, []byte("keep me"))
	orphanID, orphanIDs := storePack(t, r, []byte("throw me away"))
	saveSnapshotFor(t, r, "/src", referenced)

	gc, err := r.CollectGarbage(ctx, false)
	if err != nil {
		t.Fatalf("CollectGarbage: %v", err)
	}
	if gc.PacksDeleted != 1 {
		t.Fatalf("deleted %d packs, want 1", gc.PacksDeleted)
	}
	if gc.BytesReclaimed <= 0 {
		t.Fatalf("BytesReclaimed = %d, want > 0", gc.BytesReclaimed)
	}

	if _, err := r.Store().Stat(ctx, PackKey(orphanID)); !errs.IsNotFound(err) {
		t.Fatal("orphan pack still present after gc")
	}
	// The referenced data must survive.
	for _, id := range referenced {
		if _, err := r.GetBlob(ctx, id); err != nil {
			t.Fatalf("referenced blob %s lost to gc: %v", id.Short(), err)
		}
	}
	// And the index must no longer claim the deleted blobs.
	for _, id := range orphanIDs {
		if r.HasBlob(id) {
			t.Fatalf("index still lists blob %s from a deleted pack", id.Short())
		}
	}

	report, err := r.Verify(ctx, VerifyOptions{Full: true})
	if err != nil {
		t.Fatalf("Verify after gc: %v", err)
	}
	if !report.OK() {
		t.Fatalf("repository failed verification after gc: %s", report.Summary())
	}
}

func TestCollectGarbageDryRunDeletesNothing(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, referenced := storePack(t, r, []byte("keep"))
	orphanID, _ := storePack(t, r, []byte("orphan"))
	saveSnapshotFor(t, r, "/src", referenced)

	gc, err := r.CollectGarbage(ctx, true)
	if err != nil {
		t.Fatalf("CollectGarbage: %v", err)
	}
	if !gc.DryRun {
		t.Fatal("report does not record that it was a dry run")
	}
	if gc.PacksDeleted != 1 {
		t.Fatalf("dry run reported %d packs, want 1", gc.PacksDeleted)
	}
	if _, err := r.Store().Stat(ctx, PackKey(orphanID)); err != nil {
		t.Fatalf("dry run deleted the pack: %v", err)
	}
}

// GC must refuse to act on a damaged repository: the orphan set comes from
// the same index whose integrity is in question, so deleting on that basis
// could destroy packs a repair would need.
func TestCollectGarbageRefusesDamagedRepository(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	packID, ids := storePack(t, r, []byte("alpha"))
	saveSnapshotFor(t, r, "/src", ids)

	if err := r.Store().Delete(ctx, PackKey(packID)); err != nil {
		t.Fatalf("Delete pack: %v", err)
	}

	if _, err := r.CollectGarbage(ctx, false); err == nil {
		t.Fatal("CollectGarbage proceeded on a damaged repository")
	} else if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

// The report must be reproducible run to run, so that a diff between two
// verify outputs means something. Map iteration order is not.
func TestVerifyReportIsDeterministic(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, referenced := storePack(t, r, []byte("referenced"))
	for i := range 8 {
		storePack(t, r, []byte(fmt.Sprintf("orphan-%d", i)))
	}
	saveSnapshotFor(t, r, "/src", referenced)

	first, err := r.Verify(ctx, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	for range 5 {
		next, err := r.Verify(ctx, VerifyOptions{})
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if len(next.OrphanPacks) != len(first.OrphanPacks) {
			t.Fatal("orphan pack count varies between runs")
		}
		for i := range first.OrphanPacks {
			if next.OrphanPacks[i] != first.OrphanPacks[i] {
				t.Fatal("orphan pack ordering varies between runs")
			}
		}
	}
}
