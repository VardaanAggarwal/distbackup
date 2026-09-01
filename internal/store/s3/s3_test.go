package s3

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/retry"
	"github.com/VardaanAggarwal/distbackup/internal/store"
	"github.com/VardaanAggarwal/distbackup/internal/store/storetest"
)

func fastRetry() Option {
	return WithRetryPolicy(retry.Policy{
		MaxAttempts:        6,
		Base:               time.Microsecond,
		Max:                time.Millisecond,
		ThrottleMultiplier: 2,
		Rand:               rand.New(rand.NewSource(1)), //nolint:gosec // deterministic test jitter
	})
}

// TestConformance runs the same suite the local filesystem backend passes.
//
// This is what makes store.ObjectStore an abstraction rather than an
// aspiration: identical assertions, two completely different implementations.
func TestConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) store.ObjectStore {
		s, err := New(NewFake(), "test-bucket", fastRetry())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { s.Close() }) //nolint:errcheck // test cleanup
		return s
	})
}

// The same suite with a key prefix, so one bucket can hold several
// repositories without them seeing each other.
func TestConformanceWithPrefix(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) store.ObjectStore {
		s, err := New(NewFake(), "test-bucket", WithPrefix("repos/alpha"), fastRetry())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { s.Close() }) //nolint:errcheck // test cleanup
		return s
	})
}

// And again under constant throttling, which must be retried transparently.
func TestConformanceUnderThrottling(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) store.ObjectStore {
		s, err := New(NewFake(WithThrottleEvery(4)), "test-bucket", fastRetry())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { s.Close() }) //nolint:errcheck // test cleanup
		return s
	})
}

// D-006: 412 and 409 mean different things and must not be collapsed.
//
// A 409 says a conflicting write was in flight and the outcome is unknown, so
// it must be retried. Treating it as "already exists" would report a blob as
// stored when it may never have been written — silent data loss discovered
// only at restore.
func TestConflictIsRetriedNotTreatedAsExisting(t *testing.T) {
	f := NewFake(WithConflicts(3))
	s, err := New(f, "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	created, err := s.PutIfAbsent(ctx, "blob", []byte("payload"))
	if err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("a 409 was treated as 'already exists'; the object would never have been written")
	}
	if f.ObjectCount() != 1 {
		t.Fatalf("store holds %d objects, want 1", f.ObjectCount())
	}

	// And the bytes must actually be there.
	rc, err := s.Get(ctx, "blob")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close() //nolint:errcheck // test path
	buf := make([]byte, 7)
	if _, err := rc.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf) != "payload" {
		t.Fatalf("stored %q, want %q", buf, "payload")
	}
}

// A 412 is the settled case: the key exists, and for content-addressed data
// that is success.
func TestPreconditionFailedReportsNotCreated(t *testing.T) {
	f := NewFake()
	s, err := New(f, "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	created, err := s.PutIfAbsent(ctx, "blob", []byte("first"))
	if err != nil || !created {
		t.Fatalf("first PutIfAbsent: created=%v err=%v", created, err)
	}

	created, err = s.PutIfAbsent(ctx, "blob", []byte("second"))
	if err != nil {
		t.Fatalf("second PutIfAbsent returned an error, want (false, nil): %v", err)
	}
	if created {
		t.Fatal("second PutIfAbsent reported created == true")
	}
	if f.ObjectCount() != 1 {
		t.Fatalf("store holds %d objects, want 1", f.ObjectCount())
	}
}

// When conflicts exhaust the retry budget the caller must see a failure, not
// a false success.
func TestPersistentConflictSurfacesAsError(t *testing.T) {
	f := NewFake(WithConflicts(1000))
	s, err := New(f, "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	created, err := s.PutIfAbsent(context.Background(), "blob", []byte("payload"))
	if err == nil {
		t.Fatal("unrelenting 409s were reported as success")
	}
	if created {
		t.Fatal("created == true despite a failure")
	}
}

// Many callers racing on the same key: exactly one must be told it created
// the object, and the stored bytes must be complete.
func TestConcurrentPutIfAbsent(t *testing.T) {
	s, err := New(NewFake(), "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
}

// Byte ranges are inclusive at both ends (RFC 7233). An off-by-one here means
// the pack reader fetches one byte too many or too few, and a pack header
// fails to parse.
func TestRangeSemantics(t *testing.T) {
	s, err := New(NewFake(), "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if err := s.Put(ctx, "obj", []byte("0123456789")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cases := []struct {
		off, n int64
		want   string
	}{
		{0, 1, "0"},
		{0, 10, "0123456789"},
		{3, 4, "3456"},
		{9, 1, "9"},
		{0, -1, "0123456789"},
		{5, -1, "56789"},
		{0, 1000, "0123456789"}, // overshooting is satisfied with what exists
	}
	for _, tc := range cases {
		rc, err := s.GetRange(ctx, "obj", tc.off, tc.n)
		if err != nil {
			t.Fatalf("GetRange(%d,%d): %v", tc.off, tc.n, err)
		}
		buf := make([]byte, len(tc.want)+10)
		n, _ := rc.Read(buf)
		rc.Close() //nolint:errcheck // test path
		if got := string(buf[:n]); got != tc.want {
			t.Errorf("GetRange(%d,%d) = %q, want %q", tc.off, tc.n, got, tc.want)
		}
	}
}

// Listing must page through everything rather than stopping at the first page.
func TestListPaginates(t *testing.T) {
	// pageSize 3 against 20 objects forces at least seven pages.
	s, err := New(NewFake(WithPageSize(3)), "test-bucket", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	const n = 20
	for i := range n {
		key := "obj" + string(rune('a'+i%26)) + string(rune('0'+i/26)) + string(rune('0'+i%10))
		if err := s.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	seen := map[string]bool{}
	if err := s.List(ctx, "", func(info store.ObjectInfo) error {
		if seen[info.Key] {
			t.Errorf("key %q listed twice", info.Key)
		}
		seen[info.Key] = true
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(seen) != n {
		t.Fatalf("listed %d objects, want %d", len(seen), n)
	}
}

// A prefix must isolate repositories sharing a bucket.
func TestPrefixIsolation(t *testing.T) {
	f := NewFake()
	alpha, err := New(f, "test-bucket", WithPrefix("repos/alpha"), fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	beta, err := New(f, "test-bucket", WithPrefix("repos/beta"), fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if err := alpha.Put(ctx, "shared-name", []byte("alpha data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := beta.Put(ctx, "shared-name", []byte("beta data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	count := 0
	if err := beta.List(ctx, "", func(info store.ObjectInfo) error {
		count++
		if info.Key != "shared-name" {
			t.Errorf("beta listed %q; prefix was not trimmed", info.Key)
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if count != 1 {
		t.Fatalf("beta sees %d objects, want 1 — prefixes are not isolating", count)
	}
	if f.ObjectCount() != 2 {
		t.Fatalf("bucket holds %d objects, want 2", f.ObjectCount())
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   errs.Kind
	}{
		{404, "NoSuchKey", errs.KindNotFound},
		{412, "PreconditionFailed", errs.KindAlreadyExists},
		{409, "ConditionalRequestConflict", errs.KindTransient},
		{403, "AccessDenied", errs.KindPermission},
		{503, "SlowDown", errs.KindThrottled},
		{500, "InternalError", errs.KindTransient},
		{400, "InvalidRequest", errs.KindInvalid},
	}
	for _, tc := range cases {
		err := classify("op", &APIError{StatusCode: tc.status, Code: tc.code})
		if got := errs.KindOf(err); got != tc.want {
			t.Errorf("%d %s classified as %s, want %s", tc.status, tc.code, got, tc.want)
		}
	}

	// 409 must be retryable and 412 must not be. This is the pair that
	// matters most.
	if !errs.IsRetryable(classify("op", &APIError{StatusCode: 409})) {
		t.Error("409 must be retryable")
	}
	if errs.IsRetryable(classify("op", &APIError{StatusCode: 412})) {
		t.Error("412 must not be retryable")
	}
}

func TestNewValidatesArguments(t *testing.T) {
	if _, err := New(nil, "bucket"); err == nil {
		t.Error("New accepted a nil API")
	}
	if _, err := New(NewFake(), ""); err == nil {
		t.Error("New accepted an empty bucket")
	}
}
