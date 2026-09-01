// Package storetest provides the conformance suite that every
// store.ObjectStore implementation must pass.
//
// One suite, run against every backend including the fakes (docs/ENGINEERING-RULES.md R11).
// This is what keeps the abstraction honest: a provider is "done" when it
// passes this, and a behaviour that differs between backends either shows up
// here as a failure or is not a behaviour the engine is allowed to rely on.
//
// It matters more than usual under R7. Cloud backends are never run against a
// real service, so this suite plus a faithful fake is the *only* evidence
// that a provider behaves as the engine expects.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/store"
)

// Factory creates a fresh, empty store for one subtest.
//
// Implementations register cleanup with t.Cleanup rather than returning a
// teardown function, so a failing test cannot skip it.
type Factory func(t *testing.T) store.ObjectStore

// RunConformance runs the full suite against the store produced by newStore.
func RunConformance(t *testing.T, newStore Factory) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"PutGetRoundTrip", testPutGetRoundTrip},
		{"GetMissingIsNotFound", testGetMissingIsNotFound},
		{"PutOverwrites", testPutOverwrites},
		{"EmptyObject", testEmptyObject},
		{"LargeObject", testLargeObject},
		{"NestedKeys", testNestedKeys},
		{"GetRange", testGetRange},
		{"GetRangeToEnd", testGetRangeToEnd},
		{"GetRangePastEnd", testGetRangePastEnd},
		{"PutIfAbsentCreates", testPutIfAbsentCreates},
		{"PutIfAbsentPreservesExisting", testPutIfAbsentPreservesExisting},
		{"PutIfAbsentIsAtomicUnderRace", testPutIfAbsentIsAtomicUnderRace},
		{"Stat", testStat},
		{"StatMissingIsNotFound", testStatMissingIsNotFound},
		{"List", testList},
		{"ListPrefix", testListPrefix},
		{"ListEmpty", testListEmpty},
		{"ListPropagatesCallbackError", testListPropagatesCallbackError},
		{"Delete", testDelete},
		{"DeleteMissingIsNotAnError", testDeleteMissingIsNotAnError},
		{"InvalidKeysRejected", testInvalidKeysRejected},
		{"ContextCancellation", testContextCancellation},
		{"ConcurrentReadWrite", testConcurrentReadWrite},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore) })
	}
}

func drain(t *testing.T, rc io.ReadCloser, err error) []byte {
	t.Helper()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer rc.Close() //nolint:errcheck // test helper
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return data
}

// mustGet fetches a whole object, failing the test on any error.
func mustGet(t *testing.T, s store.ObjectStore, ctx context.Context, key string) []byte {
	t.Helper()
	rc, err := s.Get(ctx, key)
	return drain(t, rc, err)
}

// mustGetRange fetches a byte range, failing the test on any error.
func mustGetRange(t *testing.T, s store.ObjectStore, ctx context.Context, key string, off, n int64) []byte {
	t.Helper()
	rc, err := s.GetRange(ctx, key, off, n)
	return drain(t, rc, err)
}

func testPutGetRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	want := []byte("the quick brown fox")
	if err := s.Put(ctx, "obj", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := mustGet(t, s, ctx, "obj")
	if !bytes.Equal(got, want) {
		t.Fatalf("Get = %q, want %q", got, want)
	}
}

func testGetMissingIsNotFound(t *testing.T, newStore Factory) {
	s := newStore(t)
	_, err := s.Get(context.Background(), "nope")
	if !errs.IsNotFound(err) {
		t.Fatalf("kind = %s, want not_found (err: %v)", errs.KindOf(err), err)
	}
}

func testPutOverwrites(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "obj", []byte("first")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "obj", []byte("second")); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	if got := mustGet(t, s, ctx, "obj"); string(got) != "second" {
		t.Fatalf("Get = %q, want %q", got, "second")
	}
}

// An empty object is legitimate and must round-trip. Some implementations
// special-case zero length and get it wrong.
func testEmptyObject(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "empty", []byte{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGet(t, s, ctx, "empty"); len(got) != 0 {
		t.Fatalf("Get returned %d bytes, want 0", len(got))
	}
	info, err := s.Stat(ctx, "empty")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != 0 {
		t.Fatalf("Size = %d, want 0", info.Size)
	}
}

func testLargeObject(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	want := make([]byte, 4<<20)
	for i := range want {
		want[i] = byte(i * 7)
	}
	if err := s.Put(ctx, "large", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGet(t, s, ctx, "large"); !bytes.Equal(got, want) {
		t.Fatalf("large object round trip failed (%d bytes back, want %d)", len(got), len(want))
	}
}

func testNestedKeys(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	key := "packs/ab/cdef0123"
	if err := s.Put(ctx, key, []byte("nested")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGet(t, s, ctx, key); string(got) != "nested" {
		t.Fatalf("Get = %q, want %q", got, "nested")
	}
}

// GetRange is what makes the tail-header pack format cheap (D-007), so it is
// exercised at the boundaries rather than just in the middle.
func testGetRange(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	data := []byte("0123456789abcdef")
	if err := s.Put(ctx, "obj", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cases := []struct {
		off, n int64
		want   string
	}{
		{0, 4, "0123"},
		{4, 4, "4567"},
		{0, 16, "0123456789abcdef"},
		{15, 1, "f"},
		{12, 4, "cdef"},
		{8, 0, ""},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("off%d_n%d", tc.off, tc.n), func(t *testing.T) {
			got := mustGetRange(t, s, ctx, "obj", tc.off, tc.n)
			if string(got) != tc.want {
				t.Fatalf("GetRange(%d,%d) = %q, want %q", tc.off, tc.n, got, tc.want)
			}
		})
	}
}

func testGetRangeToEnd(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	data := []byte("0123456789")
	if err := s.Put(ctx, "obj", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGetRange(t, s, ctx, "obj", 6, -1); string(got) != "6789" {
		t.Fatalf("GetRange(6,-1) = %q, want %q", got, "6789")
	}
}

// Reading past the end must return the available bytes rather than failing.
// The pack reader relies on this: it guesses a tail size that may exceed the
// object.
func testGetRangePastEnd(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	data := []byte("short")
	if err := s.Put(ctx, "obj", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := mustGetRange(t, s, ctx, "obj", 0, 1000)
	if string(got) != "short" {
		t.Fatalf("GetRange past end = %q, want %q", got, "short")
	}
}

func testPutIfAbsentCreates(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	created, err := s.PutIfAbsent(ctx, "obj", []byte("v1"))
	if err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("first PutIfAbsent reported created == false")
	}
	if got := mustGet(t, s, ctx, "obj"); string(got) != "v1" {
		t.Fatalf("Get = %q, want %q", got, "v1")
	}
}

// "Already exists" must be reported as created == false with a nil error, not
// as a failure (docs/DECISIONS.md D-005), and must not modify what is stored.
func testPutIfAbsentPreservesExisting(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	if _, err := s.PutIfAbsent(ctx, "obj", []byte("original")); err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}

	created, err := s.PutIfAbsent(ctx, "obj", []byte("replacement"))
	if err != nil {
		t.Fatalf("second PutIfAbsent returned an error, want (false, nil): %v", err)
	}
	if created {
		t.Fatal("second PutIfAbsent reported created == true")
	}
	if got := mustGet(t, s, ctx, "obj"); string(got) != "original" {
		t.Fatalf("stored value = %q, want the original %q", got, "original")
	}
}

// The property the pipeline depends on: many workers racing to store the same
// content, exactly one of which is told it created the object.
func testPutIfAbsentIsAtomicUnderRace(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	const goroutines = 32
	var created atomic.Int64
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(goroutines)

	for range goroutines {
		go func() {
			defer done.Done()
			start.Wait()
			ok, err := s.PutIfAbsent(ctx, "contended", []byte("payload"))
			if err != nil {
				t.Errorf("PutIfAbsent: %v", err)
				return
			}
			if ok {
				created.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	if got := created.Load(); got != 1 {
		t.Fatalf("%d callers reported created == true, want exactly 1", got)
	}
	if got := mustGet(t, s, ctx, "contended"); string(got) != "payload" {
		t.Fatalf("stored value = %q, want %q — a partial write became visible", got, "payload")
	}
}

func testStat(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	data := []byte("0123456789")
	if err := s.Put(ctx, "obj", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	info, err := s.Stat(ctx, "obj")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", info.Size, len(data))
	}
	if info.Key != "obj" {
		t.Fatalf("Key = %q, want %q", info.Key, "obj")
	}
}

func testStatMissingIsNotFound(t *testing.T, newStore Factory) {
	s := newStore(t)
	_, err := s.Stat(context.Background(), "nope")
	if !errs.IsNotFound(err) {
		t.Fatalf("kind = %s, want not_found (err: %v)", errs.KindOf(err), err)
	}
}

func testList(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	want := []string{"a", "b/c", "b/d", "e"}
	for _, k := range want {
		if err := s.Put(ctx, k, []byte(k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	var got []string
	err := s.List(ctx, "", func(info store.ObjectInfo) error {
		got = append(got, info.Key)
		if info.Size != int64(len(info.Key)) {
			t.Errorf("object %q has size %d, want %d", info.Key, info.Size, len(info.Key))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Order is explicitly unspecified, so sort before comparing.
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("List returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List returned %v, want %v", got, want)
		}
	}
}

func testListPrefix(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	for _, k := range []string{"packs/aa/1", "packs/ab/2", "index/x", "snapshots/y"} {
		if err := s.Put(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	var got []string
	if err := s.List(ctx, "packs/", func(info store.ObjectInfo) error {
		got = append(got, info.Key)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	sort.Strings(got)
	want := []string{"packs/aa/1", "packs/ab/2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("List(\"packs/\") = %v, want %v", got, want)
	}
}

func testListEmpty(t *testing.T, newStore Factory) {
	s := newStore(t)
	count := 0
	if err := s.List(context.Background(), "", func(store.ObjectInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("List on an empty store: %v", err)
	}
	if count != 0 {
		t.Fatalf("List returned %d objects from an empty store", count)
	}
}

func testListPropagatesCallbackError(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	for i := range 5 {
		if err := s.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	sentinel := errors.New("stop listing")
	err := s.List(ctx, "", func(store.ObjectInfo) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("List returned %v, want the callback's error", err)
	}
}

func testDelete(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "obj", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "obj"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "obj"); !errs.IsNotFound(err) {
		t.Fatalf("after Delete, Get kind = %s, want not_found", errs.KindOf(err))
	}
}

// Deleting a missing key satisfies the caller's intent either way. Making it
// an error would force every caller to write the same IsNotFound check.
func testDeleteMissingIsNotAnError(t *testing.T, newStore Factory) {
	s := newStore(t)
	if err := s.Delete(context.Background(), "never-existed"); err != nil {
		t.Fatalf("Delete of a missing key returned %v, want nil", err)
	}
}

// Path traversal must be rejected by every backend, not just the filesystem
// one, so that a repository written against one store is portable to another.
func testInvalidKeysRejected(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	bad := []string{"", "/absolute", "../escape", "a/../../escape", "a//b", "a/./b", "with\x00null"}
	for _, key := range bad {
		t.Run(fmt.Sprintf("%q", key), func(t *testing.T) {
			if err := s.Put(ctx, key, []byte("x")); err == nil {
				t.Errorf("Put(%q) succeeded, want an error", key)
			}
			if _, err := s.Get(ctx, key); err == nil {
				t.Errorf("Get(%q) succeeded, want an error", key)
			}
		})
	}
}

func testContextCancellation(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Put(ctx, "obj", []byte("v")); errs.KindOf(err) != errs.KindCanceled {
		t.Errorf("Put with a canceled context: kind = %s, want canceled", errs.KindOf(err))
	}
	if _, err := s.Get(ctx, "obj"); errs.KindOf(err) != errs.KindCanceled {
		t.Errorf("Get with a canceled context: kind = %s, want canceled", errs.KindOf(err))
	}
	if _, err := s.PutIfAbsent(ctx, "obj", []byte("v")); errs.KindOf(err) != errs.KindCanceled {
		t.Errorf("PutIfAbsent with a canceled context: kind = %s, want canceled", errs.KindOf(err))
	}
}

// The pipeline calls the store from a fixed pool of workers, so every
// implementation must tolerate concurrent use. Run under -race.
func testConcurrentReadWrite(t *testing.T, newStore Factory) {
	s := newStore(t)
	ctx := context.Background()

	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(w int) {
			defer wg.Done()
			for i := range perWorker {
				key := fmt.Sprintf("w%d/k%d", w, i)
				if err := s.Put(ctx, key, []byte(key)); err != nil {
					t.Errorf("Put(%q): %v", key, err)
					return
				}
				rc, err := s.Get(ctx, key)
				if err != nil {
					t.Errorf("Get(%q): %v", key, err)
					return
				}
				got, err := io.ReadAll(rc)
				rc.Close() //nolint:errcheck // test path
				if err != nil {
					t.Errorf("read %q: %v", key, err)
					return
				}
				if string(got) != key {
					t.Errorf("object %q contains %q", key, got)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	count := 0
	if err := s.List(ctx, "", func(store.ObjectInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if want := workers * perWorker; count != want {
		t.Fatalf("store holds %d objects, want %d", count, want)
	}
}
