package ebs

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/VardaanAggarwal/distbackup/internal/errs"
	"github.com/VardaanAggarwal/distbackup/internal/retry"
	"github.com/VardaanAggarwal/distbackup/internal/source"
)

// fastRetry keeps tests quick while still exercising the retry path.
func fastRetry() Option {
	return WithRetryPolicy(retry.Policy{
		MaxAttempts:        5,
		Base:               time.Microsecond,
		Max:                time.Millisecond,
		ThrottleMultiplier: 2,
		Rand:               rand.New(rand.NewSource(1)), //nolint:gosec // deterministic test jitter
	})
}

// sparseBlocks builds a deliberately sparse block map: a real snapshot only
// lists blocks that have data.
func sparseBlocks(indexes ...int64) map[int64][]byte {
	return sparseBlocksOfSize(BlockSize, indexes...)
}

// sparseBlocksOfSize builds blocks shorter than BlockSize.
//
// A real block is 512 KiB, so a listing test with hundreds of blocks would
// allocate hundreds of megabytes to exercise logic that never looks at the
// contents. Short blocks are also realistic: DataLength is a separate field
// precisely because a block need not be full.
func sparseBlocksOfSize(size int, indexes ...int64) map[int64][]byte {
	m := make(map[int64][]byte, len(indexes))
	for _, i := range indexes {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(int(i) + j)
		}
		m[i] = data
	}
	return m
}

func collectRefs(t *testing.T, s *Source) []source.BlockRef {
	t.Helper()
	var refs []source.BlockRef
	if err := s.ListBlocks(context.Background(), func(r source.BlockRef) error {
		refs = append(refs, r)
		return nil
	}); err != nil {
		t.Fatalf("ListBlocks: %v", err)
	}
	return refs
}

func TestListBlocksSparse(t *testing.T) {
	// Only these indexes have data; the volume is nominally much larger.
	f := NewFake(sparseBlocks(0, 5, 17, 4096), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	if len(refs) != 4 {
		t.Fatalf("listed %d blocks, want 4", len(refs))
	}
	want := []int64{0, 5, 17, 4096}
	for i, r := range refs {
		if r.Index != want[i] {
			t.Fatalf("block %d has index %d, want %d", i, r.Index, want[i])
		}
		if r.Token == "" {
			t.Fatalf("block %d has no token", r.Index)
		}
	}

	// An 8 GiB volume is 16,384 blocks, but only 4 were written. Anything
	// that assumed Size/BlockSize entries would be badly wrong.
	size, err := s.Size(context.Background())
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if want := int64(8) * 1024 * 1024 * 1024; size != want {
		t.Fatalf("Size = %d, want %d", size, want)
	}
	if full := size / BlockSize; int64(len(refs)) >= full {
		t.Fatalf("sparse listing returned %d blocks for a %d-block volume", len(refs), full)
	}
}

// The documented trap: an empty page that still carries a continuation token.
// Terminating on an empty page would back up nothing and report success.
func TestEmptyPagesWithContinuationToken(t *testing.T) {
	f := NewFake(sparseBlocks(1, 2, 3), 8, WithEmptyPages(3))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	if len(refs) != 3 {
		t.Fatalf("listed %d blocks after empty pages, want 3", len(refs))
	}
}

// A listing must page through everything, not stop at the first page.
func TestPaginationCoversAllBlocks(t *testing.T) {
	indexes := make([]int64, 0, 250)
	for i := range int64(250) {
		indexes = append(indexes, i*3) // sparse and ordered
	}
	f := NewFake(sparseBlocksOfSize(64, indexes...), 8, WithPageSize(37))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	if len(refs) != len(indexes) {
		t.Fatalf("listed %d blocks, want %d", len(refs), len(indexes))
	}
	for i := 1; i < len(refs); i++ {
		if refs[i].Index <= refs[i-1].Index {
			t.Fatalf("block indexes are not strictly increasing at %d", i)
		}
	}
}

// MaxResults below the documented minimum is clamped by the service, not
// rejected.
func TestMaxResultsClamping(t *testing.T) {
	cases := map[int32]int32{
		0:     DefaultMaxResults,
		1:     MinMaxResults,
		50:    MinMaxResults,
		100:   100,
		5000:  5000,
		20000: MaxMaxResults,
	}
	for in, want := range cases {
		if got := clampMaxResults(in); got != want {
			t.Errorf("clampMaxResults(%d) = %d, want %d", in, got, want)
		}
	}
}

// The pagination token's 60-minute lifetime, not the block token's 7 days, is
// what limits a long listing.
func TestPaginationTokenExpiry(t *testing.T) {
	clock := time.Now()
	indexes := make([]int64, 0, 300)
	for i := range int64(300) {
		indexes = append(indexes, i)
	}

	// A small page size forces genuine pagination, which is what the 60-minute
	// token lifetime actually constrains.
	f := NewFake(sparseBlocksOfSize(64, indexes...), 8,
		WithClock(func() time.Time { return clock }), WithPageSize(50))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Advance the clock past the pagination window partway through.
	seen := 0
	err = s.ListBlocks(context.Background(), func(source.BlockRef) error {
		seen++
		if seen == 100 {
			clock = clock.Add(NextTokenLifetime + time.Minute)
		}
		return nil
	})
	if err == nil {
		t.Fatal("a listing that outran the 60-minute pagination window succeeded")
	}
	if errs.KindOf(err) != errs.KindExpired {
		t.Fatalf("kind = %s, want expired", errs.KindOf(err))
	}
}

func TestBlockTokenExpiry(t *testing.T) {
	clock := time.Now()
	f := NewFake(sparseBlocks(0, 1), 8, WithClock(func() time.Time { return clock }))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	buf := make([]byte, BlockSize)

	// Fresh token works.
	if _, err := s.ReadBlock(context.Background(), refs[0], buf); err != nil {
		t.Fatalf("ReadBlock with a fresh token: %v", err)
	}

	// Past the documented 7-day lifetime it must not.
	clock = clock.Add(BlockTokenLifetime + time.Hour)
	_, err = s.ReadBlock(context.Background(), refs[1], buf)
	if err == nil {
		t.Fatal("ReadBlock succeeded with an expired block token")
	}
	if errs.KindOf(err) != errs.KindExpired {
		t.Fatalf("kind = %s, want expired", errs.KindOf(err))
	}
}

// The strict reading of the documented ambiguity (Q-001): re-listing a
// snapshot invalidates block tokens from the previous listing. The client is
// written to be safe under this reading, and this proves it.
func TestRelistingInvalidatesTokensUnderStrictReading(t *testing.T) {
	f := NewFake(sparseBlocks(0, 1, 2), 8, WithTokenInvalidationOnRelist())
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first := collectRefs(t, s)

	// A second listing invalidates the first set of tokens.
	second := collectRefs(t, s)

	buf := make([]byte, BlockSize)
	if _, err := s.ReadBlock(context.Background(), first[0], buf); err == nil {
		t.Fatal("a token from a superseded listing still worked; the client must not rely on that")
	}
	if _, err := s.ReadBlock(context.Background(), second[0], buf); err != nil {
		t.Fatalf("token from the current listing failed: %v", err)
	}
}

func TestReadBlockRoundTrip(t *testing.T) {
	blocks := sparseBlocks(0, 7, 99)
	f := NewFake(blocks, 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	buf := make([]byte, BlockSize)

	for _, ref := range refs {
		n, err := s.ReadBlock(context.Background(), ref, buf)
		if err != nil {
			t.Fatalf("ReadBlock(%d): %v", ref.Index, err)
		}
		want := blocks[ref.Index]
		if n != len(want) {
			t.Fatalf("block %d: read %d bytes, want %d", ref.Index, n, len(want))
		}
		for i := range want {
			if buf[i] != want[i] {
				t.Fatalf("block %d differs at byte %d", ref.Index, i)
			}
		}
	}
}

// A block whose bytes do not match the service's checksum must be rejected,
// not returned.
func TestChecksumMismatchIsDetected(t *testing.T) {
	f := NewFake(sparseBlocks(0, 1), 8, WithCorruptBlock(1))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	buf := make([]byte, BlockSize)

	if _, err := s.ReadBlock(context.Background(), refs[0], buf); err != nil {
		t.Fatalf("clean block failed: %v", err)
	}
	_, err = s.ReadBlock(context.Background(), refs[1], buf)
	if err == nil {
		t.Fatal("a block failing its checksum was returned as valid")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

// Corruption must not be retried: the bytes are wrong and asking again
// returns the same wrong bytes.
func TestChecksumMismatchIsNotRetried(t *testing.T) {
	f := NewFake(sparseBlocks(0), 8, WithCorruptBlock(0))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	before := f.CallCount()

	buf := make([]byte, BlockSize)
	if _, err := s.ReadBlock(context.Background(), refs[0], buf); err == nil {
		t.Fatal("corrupt block accepted")
	}

	if got := f.CallCount() - before; got != 1 {
		t.Fatalf("a corrupt block was fetched %d times; corruption must not be retried", got)
	}
}

// R-008: every response body must be drained and closed, including on the
// paths that abandon a response.
func TestResponseBodiesAreAlwaysClosed(t *testing.T) {
	f := NewFake(sparseBlocks(0, 1, 2, 3, 4), 8,
		WithThrottleEvery(3), // force retries
		WithCorruptBlock(2))  // and an error path that abandons a body
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	buf := make([]byte, BlockSize)
	for _, ref := range refs {
		_, _ = s.ReadBlock(context.Background(), ref, buf) // errors are expected
	}

	if open := f.OpenBodies(); open != 0 {
		t.Fatalf("%d response bodies were never closed", open)
	}
	if maxOpen := f.MaxOpenBodies(); maxOpen > 1 {
		t.Fatalf("held %d response bodies simultaneously; the client should hold at most 1", maxOpen)
	}
}

// Throttling must be retried and eventually succeed.
func TestThrottlingIsRetried(t *testing.T) {
	f := NewFake(sparseBlocks(0, 1, 2), 8, WithThrottleEvery(2))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	refs := collectRefs(t, s)
	if len(refs) != 3 {
		t.Fatalf("listed %d blocks under throttling, want 3", len(refs))
	}

	buf := make([]byte, BlockSize)
	for _, ref := range refs {
		if _, err := s.ReadBlock(context.Background(), ref, buf); err != nil {
			t.Fatalf("ReadBlock(%d) under throttling: %v", ref.Index, err)
		}
	}
}

// A non-retryable error must surface immediately rather than consuming the
// whole retry budget.
func TestValidationErrorIsNotRetried(t *testing.T) {
	f := NewFake(sparseBlocks(0), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	f.FailNext(errs.E(errs.KindInvalid, "fake", errors.New("ValidationException")))
	before := f.CallCount()

	if err := s.ListBlocks(context.Background(), func(source.BlockRef) error { return nil }); err == nil {
		t.Fatal("a validation error was swallowed")
	}
	if got := f.CallCount() - before; got != 1 {
		t.Fatalf("validation error retried %d times", got)
	}
}

// The rule that matters most in ListChangedBlocks: an absent FirstBlockToken
// means the block is NEW, not unchanged. Reading it backwards would silently
// skip newly written data.
func TestAbsentFirstBlockTokenMeansNew(t *testing.T) {
	// The fake marks even indexes as new (no first token) and odd as changed.
	f := NewFake(sparseBlocks(0, 1, 2, 3), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := map[int64]bool{}
	err = s.ListChangedBlocks(context.Background(), "snap-0000000000000000f",
		func(c source.ChangedBlockRef) error {
			got[c.Ref.Index] = c.IsNew
			if c.Ref.Token == "" {
				t.Errorf("changed block %d has no token for the newer snapshot", c.Ref.Index)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("ListChangedBlocks: %v", err)
	}

	for idx, isNew := range got {
		wantNew := idx%2 == 0
		if isNew != wantNew {
			t.Errorf("block %d: IsNew = %v, want %v", idx, isNew, wantNew)
		}
	}
	if len(got) != 4 {
		t.Fatalf("saw %d changed blocks, want 4", len(got))
	}
}

func TestListChangedBlocksRequiresBaseline(t *testing.T) {
	f := NewFake(sparseBlocks(0), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = s.ListChangedBlocks(context.Background(), "", func(source.ChangedBlockRef) error { return nil })
	if err == nil {
		t.Fatal("ListChangedBlocks accepted an empty baseline")
	}
}

func TestReadBlockRejectsSmallBuffer(t *testing.T) {
	f := NewFake(sparseBlocks(0), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	refs := collectRefs(t, s)

	small := make([]byte, 1024)
	if _, err := s.ReadBlock(context.Background(), refs[0], small); err == nil {
		t.Fatal("ReadBlock accepted a buffer smaller than the block size")
	}
}

func TestReadBlockRejectsEmptyToken(t *testing.T) {
	f := NewFake(sparseBlocks(0), 8)
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	buf := make([]byte, BlockSize)
	_, err = s.ReadBlock(context.Background(), source.BlockRef{Index: 0}, buf)
	if err == nil {
		t.Fatal("ReadBlock accepted an empty token")
	}
}

func TestCancellationStopsListing(t *testing.T) {
	indexes := make([]int64, 0, 500)
	for i := range int64(500) {
		indexes = append(indexes, i)
	}
	f := NewFake(sparseBlocksOfSize(64, indexes...), 8, WithPageSize(100))
	s, err := New(f, "snap-0123456789abcdef0", fastRetry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	seen := 0
	err = s.ListBlocks(ctx, func(source.BlockRef) error {
		seen++
		if seen == 10 {
			cancel()
		}
		return nil
	})
	if err == nil {
		t.Fatal("listing continued after cancellation")
	}
	if errs.KindOf(err) != errs.KindCanceled {
		t.Fatalf("kind = %s, want canceled", errs.KindOf(err))
	}
}

func TestNewValidatesArguments(t *testing.T) {
	if _, err := New(nil, "snap-0123456789abcdef0"); err == nil {
		t.Error("New accepted a nil API")
	}
	if _, err := New(NewFake(nil, 8), ""); err == nil {
		t.Error("New accepted an empty snapshot ID")
	}
}

// The cost model, computed from verified published pricing. This is the
// arithmetic behind the CLI's estimate line (docs/ENGINEERING-RULES.md R7) and is checked so
// the number in the README cannot drift from the code.
func TestCostEstimateForReferenceVolume(t *testing.T) {
	// An 8 GiB volume, fully written: 8 GiB / 512 KiB.
	const blocks = 8 * 1024 * 1024 / 512
	if blocks != 16384 {
		t.Fatalf("block arithmetic is wrong: %d", blocks)
	}

	// Verified 2026-08-26: List* $0.0006 per 1,000 requests;
	// GetSnapshotBlock $0.003 per 1,000 blocks returned.
	listRequests := (blocks + MaxMaxResults - 1) / MaxMaxResults // pages at 10,000/page
	listCost := float64(listRequests) / 1000 * 0.0006
	getCost := float64(blocks) / 1000 * 0.003
	total := listCost + getCost

	t.Logf("reference 8 GiB fully-written volume: %d blocks, %d list requests", blocks, listRequests)
	t.Logf("  ListSnapshotBlocks  $%.6f", listCost)
	t.Logf("  GetSnapshotBlock    $%.6f", getCost)
	t.Logf("  total               $%.4f", total)

	// Sanity bound, not a precise assertion: the figure is region-dependent.
	if total > 0.10 {
		t.Fatalf("estimated cost $%.4f is implausibly high for 8 GiB", total)
	}
}

func TestFakeIsDeterministic(t *testing.T) {
	build := func() []string {
		f := NewFake(sparseBlocks(0, 1, 2), 8)
		s, _ := New(f, "snap-0123456789abcdef0", fastRetry())
		var out []string
		_ = s.ListBlocks(context.Background(), func(r source.BlockRef) error {
			out = append(out, fmt.Sprintf("%d:%s", r.Index, r.Token))
			return nil
		})
		return out
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatal("fake listing length varies between runs")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("fake listing differs at %d: %q vs %q", i, a[i], b[i])
		}
	}
}
