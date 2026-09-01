package repo

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/pack"
	"github.com/VardaanAggarwal/distbackup/internal/store"
	"github.com/VardaanAggarwal/distbackup/internal/store/localfs"
)

func newTestRepo(t *testing.T) *Repository {
	t.Helper()
	s, err := localfs.Open(t.TempDir(), localfs.WithoutFsync())
	if err != nil {
		t.Fatalf("localfs.Open: %v", err)
	}
	r, err := Create(context.Background(), s, DefaultConfig())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { r.Close() }) //nolint:errcheck // test cleanup
	return r
}

// storePack builds a pack from the given payloads and stores it.
func storePack(t *testing.T, r *Repository, payloads ...[]byte) (blob.ID, []blob.ID) {
	t.Helper()

	var buf bytes.Buffer
	w := pack.NewWriter(&buf)
	ids := make([]blob.ID, 0, len(payloads))

	for _, p := range payloads {
		id := blob.Compute(p)
		if _, err := w.Add(id, p); err != nil {
			t.Fatalf("pack.Add: %v", err)
		}
		ids = append(ids, id)
	}

	packID, hdr, err := w.Finish()
	if err != nil {
		t.Fatalf("pack.Finish: %v", err)
	}
	if err := r.PutPack(context.Background(), packID, buf.Bytes(), hdr); err != nil {
		t.Fatalf("PutPack: %v", err)
	}
	return packID, ids
}

func TestCreateAndOpen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := localfs.Open(dir, localfs.WithoutFsync())
	if err != nil {
		t.Fatalf("localfs.Open: %v", err)
	}
	if _, err := Create(ctx, s, DefaultConfig()); err != nil {
		t.Fatalf("Create: %v", err)
	}

	s2, err := localfs.Open(dir, localfs.WithoutFsync())
	if err != nil {
		t.Fatalf("localfs.Open: %v", err)
	}
	r, err := Open(ctx, s2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.Config().FormatVersion != FormatVersion {
		t.Fatalf("format version = %d, want %d", r.Config().FormatVersion, FormatVersion)
	}
}

// Creating over an existing repository must fail rather than overwrite the
// settings its data was written with.
func TestCreateRefusesExistingRepository(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, _ := localfs.Open(dir, localfs.WithoutFsync())
	if _, err := Create(ctx, s, DefaultConfig()); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if _, err := Create(ctx, s, DefaultConfig()); err == nil {
		t.Fatal("second Create succeeded; it must refuse an existing repository")
	}
}

func TestOpenMissingRepository(t *testing.T) {
	s, _ := localfs.Open(t.TempDir(), localfs.WithoutFsync())
	_, err := Open(context.Background(), s)
	if !errs.IsNotFound(err) {
		t.Fatalf("kind = %s, want not_found", errs.KindOf(err))
	}
}

// An unknown format version must be refused rather than guessed at.
func TestUnknownFormatVersionRefused(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, _ := localfs.Open(dir, localfs.WithoutFsync())
	if _, err := Create(ctx, s, DefaultConfig()); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Rewrite config with a future version.
	future := DefaultConfig()
	future.FormatVersion = 99
	data := fmt.Sprintf(`{"format_version":%d,"created_at":"2026-01-01T00:00:00Z",
		"chunker_min_size":%d,"chunker_avg_size":%d,"chunker_max_size":%d,
		"chunker_normalization":%d,"pack_target_size":%d}`,
		future.FormatVersion, future.ChunkerMinSize, future.ChunkerAvgSize,
		future.ChunkerMaxSize, future.ChunkerNormalization, future.PackTargetSize)
	if err := s.Put(ctx, ConfigKey, []byte(data)); err != nil {
		t.Fatalf("Put config: %v", err)
	}

	_, err := Open(ctx, s)
	if err == nil {
		t.Fatal("Open accepted an unknown format version")
	}
	if errs.KindOf(err) != errs.KindUnsupported {
		t.Fatalf("kind = %s, want unsupported", errs.KindOf(err))
	}
}

func TestPutPackAndGetBlob(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	payloads := [][]byte{[]byte("first blob"), []byte("second blob"), bytes.Repeat([]byte("x"), 5000)}
	_, ids := storePack(t, r, payloads...)

	for i, id := range ids {
		got, err := r.GetBlob(ctx, id)
		if err != nil {
			t.Fatalf("GetBlob(%s): %v", id.Short(), err)
		}
		if !bytes.Equal(got, payloads[i]) {
			t.Fatalf("blob %d round-tripped incorrectly", i)
		}
		if !r.HasBlob(id) {
			t.Fatalf("HasBlob(%s) = false", id.Short())
		}
	}
}

func TestGetBlobNotFound(t *testing.T) {
	r := newTestRepo(t)
	_, err := r.GetBlob(context.Background(), blob.Compute([]byte("absent")))
	if !errs.IsNotFound(err) {
		t.Fatalf("kind = %s, want not_found", errs.KindOf(err))
	}
}

// A blob whose stored bytes were altered must be caught on read, not returned.
func TestGetBlobDetectsCorruption(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("corrupt me"), 100)
	packID, ids := storePack(t, r, payload)

	// Flip a byte inside the pack's data region.
	full, err := readWholeObject(ctx, r.Store(), PackKey(packID))
	if err != nil {
		t.Fatalf("read pack: %v", err)
	}
	full[10] ^= 0xFF
	if err := r.Store().Put(ctx, PackKey(packID), full); err != nil {
		t.Fatalf("Put corrupted pack: %v", err)
	}

	if _, err := r.GetBlob(ctx, ids[0]); !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func readWholeObject(ctx context.Context, s store.ObjectStore, key string) ([]byte, error) {
	rc, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close() //nolint:errcheck // test helper
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestIndexSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, _ := localfs.Open(dir, localfs.WithoutFsync())
	r, err := Create(ctx, s, DefaultConfig())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, ids := storePack(t, r, []byte("a"), []byte("b"), []byte("c"))
	if err := r.SaveIndex(ctx); err != nil {
		t.Fatalf("SaveIndex: %v", err)
	}

	s2, _ := localfs.Open(dir, localfs.WithoutFsync())
	r2, err := Open(ctx, s2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range ids {
		if !r2.HasBlob(id) {
			t.Fatalf("blob %s missing after reopen", id.Short())
		}
	}
}

// The index is a cache. Losing it must cost time, not data — this is the
// property that justifies putting the pack header at the end (D-007).
func TestRebuildIndexFromPacks(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, _ := localfs.Open(dir, localfs.WithoutFsync())
	r, err := Create(ctx, s, DefaultConfig())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var allIDs []blob.ID
	for i := range 5 {
		_, ids := storePack(t, r,
			[]byte(fmt.Sprintf("pack%d-blob0", i)),
			[]byte(fmt.Sprintf("pack%d-blob1", i)))
		allIDs = append(allIDs, ids...)
	}
	if err := r.SaveIndex(ctx); err != nil {
		t.Fatalf("SaveIndex: %v", err)
	}

	// Destroy the index entirely.
	if err := s.Delete(ctx, IndexKey); err != nil {
		t.Fatalf("Delete index: %v", err)
	}

	s2, _ := localfs.Open(dir, localfs.WithoutFsync())
	r2, err := Open(ctx, s2)
	if err != nil {
		t.Fatalf("Open after index loss: %v", err)
	}
	for _, id := range allIDs {
		if !r2.HasBlob(id) {
			t.Fatalf("blob %s missing after index rebuild", id.Short())
		}
		if _, err := r2.GetBlob(ctx, id); err != nil {
			t.Fatalf("GetBlob(%s) after rebuild: %v", id.Short(), err)
		}
	}
}

// A corrupt index must be discarded and rebuilt, not treated as fatal.
func TestCorruptIndexIsRebuilt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, _ := localfs.Open(dir, localfs.WithoutFsync())
	r, err := Create(ctx, s, DefaultConfig())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, ids := storePack(t, r, []byte("data1"), []byte("data2"))
	if err := r.SaveIndex(ctx); err != nil {
		t.Fatalf("SaveIndex: %v", err)
	}

	if err := s.Put(ctx, IndexKey, []byte("this is not an index")); err != nil {
		t.Fatalf("Put corrupt index: %v", err)
	}

	s2, _ := localfs.Open(dir, localfs.WithoutFsync())
	r2, err := Open(ctx, s2)
	if err != nil {
		t.Fatalf("Open with a corrupt index: %v", err)
	}
	for _, id := range ids {
		if !r2.HasBlob(id) {
			t.Fatalf("blob %s missing after rebuilding from a corrupt index", id.Short())
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, ids := storePack(t, r, []byte("file content"))

	snap := &Snapshot{
		Kind:      KindFiles,
		CreatedAt: time.Now().UTC().Truncate(time.Second),
		Source:    "/some/dir",
		Files: []FileEntry{{
			Path:   "a.txt",
			Size:   12,
			Mode:   0o644,
			Chunks: ids,
		}},
	}
	if err := r.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if snap.ID == "" {
		t.Fatal("SaveSnapshot did not assign an ID")
	}

	loaded, err := r.LoadSnapshot(ctx, snap.ID)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded.ID != snap.ID {
		t.Fatalf("ID = %s, want %s", loaded.ID, snap.ID)
	}
	if len(loaded.Files) != 1 || loaded.Files[0].Path != "a.txt" {
		t.Fatalf("loaded files = %+v", loaded.Files)
	}
}

// The snapshot ID is the manifest's content address, which gives integrity
// checking for free: altered bytes fail to verify.
func TestSnapshotTamperingIsDetected(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, ids := storePack(t, r, []byte("content"))
	snap := &Snapshot{
		Kind:      KindFiles,
		CreatedAt: time.Now().UTC(),
		Source:    "/x",
		Files:     []FileEntry{{Path: "a", Size: 7, Chunks: ids}},
	}
	if err := r.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	raw, err := readWholeObject(ctx, r.Store(), snap.Key())
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	tampered := bytes.Replace(raw, []byte(`"path": "a"`), []byte(`"path": "b"`), 1)
	if bytes.Equal(tampered, raw) {
		t.Fatal("test setup failed: could not alter the manifest")
	}
	if err := r.Store().Put(ctx, snap.Key(), tampered); err != nil {
		t.Fatalf("Put tampered snapshot: %v", err)
	}

	if _, err := r.LoadSnapshot(ctx, snap.ID); !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func TestSnapshotValidation(t *testing.T) {
	cases := map[string]*Snapshot{
		"unknown kind":      {Kind: "nonsense"},
		"files with blocks": {Kind: KindFiles, Blocks: []BlockEntry{{Index: 0, Blob: blob.Compute([]byte("x"))}}},
		"blocks with files": {Kind: KindBlocks, BlockSize: 512, Files: []FileEntry{{Path: "a"}}},
		"blocks no size":    {Kind: KindBlocks},
		"file no path":      {Kind: KindFiles, Files: []FileEntry{{Path: ""}}},
		"negative size":     {Kind: KindFiles, Files: []FileEntry{{Path: "a", Size: -1}}},
		"negative index":    {Kind: KindBlocks, BlockSize: 512, Blocks: []BlockEntry{{Index: -1, Blob: blob.Compute([]byte("x"))}}},
		"zero blob":         {Kind: KindBlocks, BlockSize: 512, Blocks: []BlockEntry{{Index: 0}}},
	}
	for name, snap := range cases {
		t.Run(name, func(t *testing.T) {
			if err := snap.Validate(); err == nil {
				t.Fatal("Validate accepted an invalid snapshot")
			}
		})
	}
}

func TestListSnapshots(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	_, ids := storePack(t, r, []byte("x"))
	for i := range 3 {
		snap := &Snapshot{
			Kind:      KindFiles,
			CreatedAt: time.Now().UTC().Add(time.Duration(i) * time.Hour),
			Source:    fmt.Sprintf("/src%d", i),
			Files:     []FileEntry{{Path: fmt.Sprintf("f%d", i), Size: 1, Chunks: ids}},
		}
		if err := r.SaveSnapshot(ctx, snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
	}

	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("got %d snapshots, want 3", len(snaps))
	}
	// Sorted by ID, which is stable regardless of the writer's clock.
	for i := 1; i < len(snaps); i++ {
		if snaps[i-1].ID >= snaps[i].ID {
			t.Fatal("snapshots are not sorted by ID")
		}
	}
}

func TestBlobIDsDeduplicates(t *testing.T) {
	shared := blob.Compute([]byte("shared"))
	unique := blob.Compute([]byte("unique"))

	snap := &Snapshot{
		Kind: KindFiles,
		Files: []FileEntry{
			{Path: "a", Chunks: []blob.ID{shared, unique}},
			{Path: "b", Chunks: []blob.ID{shared, shared}},
		},
	}
	ids := snap.BlobIDs()
	if len(ids) != 2 {
		t.Fatalf("BlobIDs returned %d ids, want 2 distinct", len(ids))
	}
}

func TestDedupRatio(t *testing.T) {
	if got := (Stats{}).DedupRatio(); got != 0 {
		t.Fatalf("empty stats DedupRatio = %v, want 0", got)
	}
	s := Stats{BytesNew: 25, BytesDeduped: 75}
	if got := s.DedupRatio(); got != 0.75 {
		t.Fatalf("DedupRatio = %v, want 0.75", got)
	}
}
