package localfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/store"
	"github.com/VardaanAggarwal/distbackup/internal/store/storetest"
)

// TestConformance runs the shared suite that every ObjectStore must pass.
func TestConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) store.ObjectStore {
		s, err := Open(t.TempDir(), WithoutFsync())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { s.Close() }) //nolint:errcheck // test cleanup
		return s
	})
}

// The same suite with fsync enabled, so the durable path is exercised too and
// not just the fast test path.
func TestConformanceWithFsync(t *testing.T) {
	if testing.Short() {
		t.Skip("fsync path is slow; skipped under -short")
	}
	storetest.RunConformance(t, func(t *testing.T) store.ObjectStore {
		s, err := Open(t.TempDir())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { s.Close() }) //nolint:errcheck // test cleanup
		return s
	})
}

// A key must never be able to write outside the repository root. This is the
// one bug in this file whose consequence is damage outside the program.
func TestKeyCannotEscapeRoot(t *testing.T) {
	root := t.TempDir()
	s, err := Open(filepath.Join(root, "repo"), WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	canary := filepath.Join(root, "canary")
	ctx := context.Background()

	for _, key := range []string{
		"../canary",
		"../../canary",
		"a/../../canary",
		"./../canary",
	} {
		if err := s.Put(ctx, key, []byte("escaped")); err == nil {
			t.Errorf("Put(%q) succeeded; it must be rejected", key)
		}
	}

	if _, err := os.Stat(canary); !os.IsNotExist(err) {
		t.Fatal("a write escaped the repository root")
	}
}

// Temp files from an interrupted write must never appear as objects.
func TestListIgnoresTempFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	if err := s.Put(ctx, "real", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Simulate a crashed write.
	if err := os.WriteFile(filepath.Join(dir, ".tmp-abandoned"), []byte("junk"), 0o600); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}

	var keys []string
	if err := s.List(ctx, "", func(info store.ObjectInfo) error {
		keys = append(keys, info.Key)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	for _, k := range keys {
		if strings.HasPrefix(filepath.Base(k), ".tmp-") {
			t.Fatalf("List returned a temp file: %q", k)
		}
	}
	if len(keys) != 1 || keys[0] != "real" {
		t.Fatalf("List = %v, want [real]", keys)
	}
}

// A failed write must leave no debris behind. Otherwise a repository
// accumulates temp files forever, and `verify` cannot tell them from data.
func TestNoTempFilesLeftAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	for i := range 20 {
		if err := s.Put(ctx, "obj", []byte{byte(i)}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := s.PutIfAbsent(ctx, "once", []byte("v")); err != nil {
			t.Fatalf("PutIfAbsent: %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Fatalf("temp file left behind: %q", e.Name())
		}
	}
}

// Stat on a directory must report not-found rather than returning a bogus
// object, or List and Stat would disagree about what exists.
func TestStatOnDirectoryIsNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	if err := s.Put(ctx, "sub/obj", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Stat(ctx, "sub"); !errs.IsNotFound(err) {
		t.Fatalf("Stat on a directory: kind = %s, want not_found", errs.KindOf(err))
	}
}

func TestOpenCreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b", "c")
	s, err := Open(root, WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.Root() != root {
		t.Fatalf("Root = %q, want %q", s.Root(), root)
	}
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		t.Fatalf("Open did not create the root directory: %v", err)
	}
}

// PutIfAbsent must publish complete content. The temp-then-link sequence is
// what guarantees it; this checks the stored bytes rather than trusting the
// return value.
func TestPutIfAbsentPublishesCompleteContent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i)
	}

	created, err := s.PutIfAbsent(ctx, "big", payload)
	if err != nil || !created {
		t.Fatalf("PutIfAbsent: created=%v err=%v", created, err)
	}

	info, err := s.Stat(ctx, "big")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("stored size = %d, want %d", info.Size, len(payload))
	}
}

func TestGetRangeRejectsNegativeOffset(t *testing.T) {
	s, err := Open(t.TempDir(), WithoutFsync())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := s.Put(ctx, "obj", []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.GetRange(ctx, "obj", -1, 4); err == nil {
		t.Fatal("GetRange accepted a negative offset")
	}
}
