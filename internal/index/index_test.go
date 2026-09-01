package index

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VardaanAggarwal/distbackup/internal/blob"
	"github.com/VardaanAggarwal/distbackup/internal/errs"
)

func testLoc(seed int) Location {
	return Location{
		PackID: blob.Compute(fmt.Appendf(nil, "pack-%d", seed)),
		Offset: int64(seed) * 1024,
		Length: 4096,
	}
}

// TestConcurrentDedup is mandatory (docs/ENGINEERING-RULES.md R6) and must never be weakened.
//
// It is the test that proves Insert's check-and-set is atomic. If the
// implementation ever regresses to read-lock-then-upgrade, two goroutines can
// both observe the blob as absent and both report true — and two pipeline
// workers would then both store the same blob, defeating deduplication in
// precisely the concurrent case the system exists to handle.
//
// The barrier matters: without it the goroutines start staggered and the race
// window is rarely hit, so the test would pass against broken code.
func TestConcurrentDedup(t *testing.T) {
	const goroutines = 100

	// Repeat the whole experiment: a single round can get lucky even with a
	// broken implementation.
	for round := range 50 {
		idx := New()
		id := blob.Compute(fmt.Appendf(nil, "contended-blob-%d", round))

		var inserted atomic.Int64
		var start sync.WaitGroup
		var done sync.WaitGroup
		start.Add(1)
		done.Add(goroutines)

		for g := range goroutines {
			go func(g int) {
				defer done.Done()
				start.Wait() // release all goroutines at once
				if idx.Insert(id, testLoc(g)) {
					inserted.Add(1)
				}
			}(g)
		}

		start.Done()
		done.Wait()

		if got := inserted.Load(); got != 1 {
			t.Fatalf("round %d: %d goroutines reported inserted==true, want exactly 1", round, got)
		}
		if idx.Len() != 1 {
			t.Fatalf("round %d: index holds %d entries, want 1", round, idx.Len())
		}
		if !idx.Contains(id) {
			t.Fatalf("round %d: blob missing after concurrent insertion", round)
		}
	}
}

// Concurrent insertion of *distinct* blobs must not lose any of them.
func TestConcurrentDistinctInserts(t *testing.T) {
	const goroutines = 64
	const perGoroutine = 200

	idx := New()
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				id := blob.Compute(fmt.Appendf(nil, "blob-%d-%d", g, i))
				if !idx.Insert(id, testLoc(i)) {
					t.Errorf("distinct blob reported as duplicate")
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if want := goroutines * perGoroutine; idx.Len() != want {
		t.Fatalf("index holds %d entries, want %d", idx.Len(), want)
	}
}

// Mixed concurrent readers and writers, run under -race to catch any missing
// synchronisation on the read path.
func TestConcurrentReadWrite(t *testing.T) {
	idx := New()
	const n = 500

	ids := make([]blob.ID, n)
	for i := range ids {
		ids[i] = blob.Compute(fmt.Appendf(nil, "rw-%d", i))
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i, id := range ids {
			idx.Insert(id, testLoc(i))
		}
	}()
	go func() {
		defer wg.Done()
		for _, id := range ids {
			idx.Lookup(id)
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			idx.Len()
			idx.ShardStats()
		}
	}()

	wg.Wait()

	if idx.Len() != n {
		t.Fatalf("index holds %d entries, want %d", idx.Len(), n)
	}
}

func TestInsertAndLookup(t *testing.T) {
	idx := New()
	id := blob.Compute([]byte("hello"))
	loc := testLoc(7)

	if _, ok := idx.Lookup(id); ok {
		t.Fatal("empty index reported a hit")
	}
	if !idx.Insert(id, loc) {
		t.Fatal("first Insert returned false")
	}
	got, ok := idx.Lookup(id)
	if !ok {
		t.Fatal("Lookup missed a blob that was inserted")
	}
	if got != loc {
		t.Fatalf("Lookup = %+v, want %+v", got, loc)
	}
}

// A second insert must not overwrite. A blob's content address determines its
// bytes, so any stored copy is as good as any other, and rewriting the
// location would invalidate references other code may already hold.
func TestInsertDoesNotOverwrite(t *testing.T) {
	idx := New()
	id := blob.Compute([]byte("stable"))
	first := testLoc(1)
	second := testLoc(2)

	idx.Insert(id, first)
	if idx.Insert(id, second) {
		t.Fatal("second Insert returned true")
	}

	got, _ := idx.Lookup(id)
	if got != first {
		t.Fatalf("location changed to %+v, want the original %+v", got, first)
	}
}

func TestDelete(t *testing.T) {
	idx := New()
	id := blob.Compute([]byte("temp"))

	if idx.Delete(id) {
		t.Fatal("Delete of an absent blob returned true")
	}
	idx.Insert(id, testLoc(1))
	if !idx.Delete(id) {
		t.Fatal("Delete of a present blob returned false")
	}
	if idx.Contains(id) {
		t.Fatal("blob still present after Delete")
	}
	if idx.Len() != 0 {
		t.Fatalf("Len = %d after deleting the only entry", idx.Len())
	}
}

func TestForEach(t *testing.T) {
	idx := New()
	const n = 100
	want := make(map[blob.ID]Location, n)

	for i := range n {
		id := blob.Compute(fmt.Appendf(nil, "each-%d", i))
		loc := testLoc(i)
		idx.Insert(id, loc)
		want[id] = loc
	}

	got := make(map[blob.ID]Location, n)
	err := idx.ForEach(func(id blob.ID, loc Location) error {
		got[id] = loc
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("visited %d entries, want %d", len(got), len(want))
	}
	for id, loc := range want {
		if got[id] != loc {
			t.Fatalf("entry %s = %+v, want %+v", id.Short(), got[id], loc)
		}
	}
}

func TestForEachPropagatesError(t *testing.T) {
	idx := New()
	for i := range 10 {
		idx.Insert(blob.Compute(fmt.Appendf(nil, "e-%d", i)), testLoc(i))
	}

	sentinel := fmt.Errorf("stop")
	err := idx.ForEach(func(blob.ID, Location) error { return sentinel })
	if err != sentinel { //nolint:errorlint // identity comparison is the point
		t.Fatalf("got %v, want the sentinel", err)
	}
}

// The design claims SHA-256's first byte is a good shard selector. This
// measures it rather than assuming it — if the distribution were skewed, lock
// contention would be quietly worse than the design predicts.
func TestShardBalance(t *testing.T) {
	idx := New()
	const n = 256 * 400 // 400 per shard on average

	for i := range n {
		idx.Insert(blob.Compute(fmt.Appendf(nil, "balance-%d", i)), testLoc(i))
	}

	stats := idx.ShardStats()
	mean := float64(n) / float64(NumShards)

	minCount, maxCount := stats[0], stats[0]
	var sumSq float64
	for _, c := range stats {
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
		d := float64(c) - mean
		sumSq += d * d
	}
	stddev := math.Sqrt(sumSq / float64(NumShards))

	t.Logf("shard balance over %d entries across %d shards:", n, NumShards)
	t.Logf("  mean %.1f, min %d, max %d, stddev %.1f (%.1f%% of mean)",
		mean, minCount, maxCount, stddev, stddev/mean*100)

	// For a uniform distribution the counts are ~Poisson, so stddev ≈ sqrt(mean).
	// Allow 3x that before calling the shard key bad.
	if limit := 3 * math.Sqrt(mean); stddev > limit {
		t.Fatalf("shard stddev %.1f exceeds %.1f; the first byte is not distributing evenly", stddev, limit)
	}
	if minCount == 0 {
		t.Fatal("at least one shard is empty; shard selection is not covering the key space")
	}
}

func TestSerializationRoundTrip(t *testing.T) {
	idx := New()
	const n = 5000
	want := make(map[blob.ID]Location, n)

	for i := range n {
		id := blob.Compute(fmt.Appendf(nil, "ser-%d", i))
		loc := testLoc(i)
		idx.Insert(id, loc)
		want[id] = loc
	}

	var buf bytes.Buffer
	written, err := idx.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if wantBytes := int64(indexHeaderSize + n*entrySize); written != wantBytes {
		t.Fatalf("wrote %d bytes, want %d", written, wantBytes)
	}

	restored, err := ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if restored.Len() != n {
		t.Fatalf("restored %d entries, want %d", restored.Len(), n)
	}
	for id, loc := range want {
		got, ok := restored.Lookup(id)
		if !ok {
			t.Fatalf("blob %s missing after round trip", id.Short())
		}
		if got != loc {
			t.Fatalf("blob %s = %+v, want %+v", id.Short(), got, loc)
		}
	}
}

func TestSerializationEmptyIndex(t *testing.T) {
	var buf bytes.Buffer
	if _, err := New().WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	restored, err := ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if restored.Len() != 0 {
		t.Fatalf("restored %d entries from an empty index", restored.Len())
	}
}

func TestReadFromRejectsBadMagic(t *testing.T) {
	var buf bytes.Buffer
	if _, err := New().WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	data := buf.Bytes()
	data[0] ^= 0xFF

	_, err := ReadFrom(bytes.NewReader(data))
	if err == nil {
		t.Fatal("ReadFrom accepted bad magic")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func TestReadFromRejectsTruncation(t *testing.T) {
	idx := New()
	for i := range 100 {
		idx.Insert(blob.Compute(fmt.Appendf(nil, "t-%d", i)), testLoc(i))
	}
	var buf bytes.Buffer
	if _, err := idx.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	full := buf.Bytes()
	for _, keep := range []int{0, 3, indexHeaderSize, indexHeaderSize + 1, len(full) - 1} {
		t.Run(fmt.Sprintf("keep_%d", keep), func(t *testing.T) {
			_, err := ReadFrom(bytes.NewReader(full[:keep]))
			if err == nil {
				t.Fatal("ReadFrom accepted a truncated index")
			}
		})
	}
}

// A corrupted entry count must not become a huge allocation.
func TestReadFromRejectsAbsurdCount(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(indexMagic[:])
	countBuf := make([]byte, 8)
	for i := range countBuf {
		countBuf[i] = 0xFF
	}
	buf.Write(countBuf)

	_, err := ReadFrom(&buf)
	if err == nil {
		t.Fatal("ReadFrom accepted an absurd entry count")
	}
	if !errs.IsCorrupt(err) {
		t.Fatalf("kind = %s, want corrupt", errs.KindOf(err))
	}
}

func TestReadFromRejectsTrailingBytes(t *testing.T) {
	idx := New()
	idx.Insert(blob.Compute([]byte("one")), testLoc(1))

	var buf bytes.Buffer
	if _, err := idx.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	buf.WriteString("extra")

	if _, err := ReadFrom(&buf); err == nil {
		t.Fatal("ReadFrom accepted trailing bytes after the declared count")
	}
}

func BenchmarkIndexInsert(b *testing.B) {
	ids := make([]blob.ID, b.N)
	for i := range ids {
		ids[i] = blob.Compute(fmt.Appendf(nil, "bench-%d", i))
	}
	idx := New()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		idx.Insert(ids[i], Location{Offset: int64(i), Length: 4096})
	}
}

func BenchmarkIndexLookupHit(b *testing.B) {
	const n = 100000
	idx := New()
	ids := make([]blob.ID, n)
	for i := range ids {
		ids[i] = blob.Compute(fmt.Appendf(nil, "lookup-%d", i))
		idx.Insert(ids[i], testLoc(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		idx.Lookup(ids[i%n])
	}
}

// idPool is precomputed outside the timed region on purpose.
//
// An earlier version of these benchmarks called blob.Compute inside the
// parallel loop, and measured 513 ns/op sharded against 525 ns/op
// single-mutex — an apparently pointless optimisation. The benchmark was
// wrong, not the design: SHA-256 of the key dominated, so both variants were
// really measuring hashing rather than lock contention. Precomputing the IDs
// isolates what the sharding actually changes.
func idPool(n int) []blob.ID {
	ids := make([]blob.ID, n)
	for i := range ids {
		ids[i] = blob.Compute(fmt.Appendf(nil, "par-%d", i))
	}
	return ids
}

// The number that justifies sharding: parallel inserts across many goroutines.
func BenchmarkIndexInsertParallel(b *testing.B) {
	ids := idPool(1 << 20)
	idx := New()
	var counter atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			idx.Insert(ids[int(i)&(len(ids)-1)], Location{Offset: i})
		}
	})
}

// The comparison case: a single map behind a single mutex, which is what
// sharding replaces. Run both to see the difference.
func BenchmarkSingleMutexInsertParallel(b *testing.B) {
	ids := idPool(1 << 20)
	var mu sync.Mutex
	m := make(map[blob.ID]Location)
	var counter atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			id := ids[int(i)&(len(ids)-1)]
			mu.Lock()
			if _, ok := m[id]; !ok {
				m[id] = Location{Offset: i}
			}
			mu.Unlock()
		}
	})
}

// Lookup is the hot path: every chunk the pipeline produces performs one, and
// most of them hit. This measures read-lock contention specifically.
func BenchmarkIndexLookupParallel(b *testing.B) {
	ids := idPool(1 << 20)
	idx := New()
	for i, id := range ids {
		idx.Insert(id, Location{Offset: int64(i)})
	}
	var counter atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			idx.Lookup(ids[int(i)&(len(ids)-1)])
		}
	})
}

func BenchmarkSingleMutexLookupParallel(b *testing.B) {
	ids := idPool(1 << 20)
	var mu sync.RWMutex
	m := make(map[blob.ID]Location, len(ids))
	for i, id := range ids {
		m[id] = Location{Offset: int64(i)}
	}
	var counter atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			mu.RLock()
			_ = m[ids[int(i)&(len(ids)-1)]]
			mu.RUnlock()
		}
	})
}
