# Phases 2–3 — Pack format and sharded index

## 1. What was built

**`internal/pack`** — the pack file format. A `Writer` appends blobs and emits a trailing JSON header plus a fixed 12-byte trailer; a `Reader` parses that header via random access and reads individual blobs back, verified. `ParseTail` recovers a pack's contents from a single ranged read of its tail, with no index.

**`internal/index`** — the deduplication index: blob ID → `{PackID, Offset, Length}`, split across 256 independently-locked shards.

## 2. Key decisions

**Pack ID is the SHA-256 of the pack's own bytes.** Makes an upload idempotent — a retry writes the identical object under the identical key — and gives `verify` a way to detect a pack whose bytes changed underneath the repository.

**The writer verifies each blob against its declared ID before storing it.** *Rejected:* trusting the caller. A caller that computed the ID from different bytes than it stored would produce a pack that fails at restore time, months later, with no way to trace it back to the write.

**The writer deduplicates within a pack.** The index is consulted before a blob reaches the writer, but two identical chunks can be in flight concurrently and both miss. The writer is the last place to catch it before it costs storage.

**Magic is checked before the length field is trusted.** A wrong magic means the file is not a pack, so its "length" is meaningless. `MaxHeaderSize` then bounds the allocation. Together these turn a corrupt file into an error instead of an OOM.

**`ErrShortTail` carries the exact byte count needed.** The tail-recovery path guesses 64 KiB. When that is too small the error says precisely how far back to read, so the retry is guaranteed rather than another guess — and the retry is bounded at one, because a second short tail would mean the pack changed underneath us.

**Index shard key is the first byte of the blob ID — no hash function.** Blob IDs are SHA-256 digests, uniformly distributed by construction. The content address *is* the hash. 256 shards, one per byte value, selected by a single array index.

**`Insert` does check-and-set under one write lock.** *Rejected:* read-lock, release, write-lock, insert. It looks like an optimisation for the common already-present case and it is wrong — two goroutines can both see the blob as absent in the gap and both report `true`, so two workers both store it. See §4 for proof the test catches this.

**`Len()` locks shards one at a time and is therefore approximate under concurrent writes.** Used for progress and statistics, never for a correctness decision. Holding all 256 locks at once would stall every worker to produce a number nobody needs exact.

## 3. How it works

A pack is written forward in one pass: blob bytes, then blob bytes, then the JSON header listing `{id, offset, length}` for each, then a `uint32` header length, then the 8-byte magic. Nothing needs to be known in advance, so memory stays bounded and the pack ID falls out of the same hash that streamed the bytes.

Reading inverts it. Fetch the last 64 KiB, check the magic in the final 8 bytes, read the length from the 4 before it, and the header is the `headerLen` bytes preceding that. Every entry is then bounds-checked against the end of the blob region *before* any read is attempted, so a corrupt offset can never turn into an out-of-range access.

The index is 256 `{sync.RWMutex, map[blob.ID]Location}` pairs. `shardFor(id)` is `&idx.shards[id[0]]`. That is the whole trick — because the key is already a uniform hash, sharding costs one array index and zero arithmetic.

## 4. Test output

```
$ go test -race ./...
TEST EXIT: 0

$ golangci-lint run ./...
LINT EXIT: 0
```

Mandatory concurrent dedup test (CLAUDE.md R6) — 100 goroutines released simultaneously from a barrier, repeated over 50 rounds:

```
--- PASS: TestConcurrentDedup (0.04s)
```

**Proof that the test discriminates.** `Insert` was temporarily replaced with the read-lock-then-upgrade pattern the doc comment rejects:

```
--- FAIL: TestConcurrentDedup (0.00s)
    index_test.go:62: round 2: 2 goroutines reported inserted==true, want exactly 1
```

It fails within three rounds. A single round would have passed — which is why the test repeats.

Shard balance over 102,400 entries:

```
mean 400.0, min 345, max 456, stddev 20.3 (5.1% of mean)
```

For a uniform distribution the counts are Poisson, so the predicted stddev is √400 = 20.0. Measured 20.3. The claim that SHA-256's first byte is a good shard selector is not an assumption here; it is measured to match theory within 1.5%.

## 5. Measurements

Apple M1, darwin/arm64, Go 1.23.4, 2026-08-26.

```
$ go test -bench='Parallel' -benchmem -run='^$' ./internal/index/
BenchmarkIndexInsertParallel-8         16559722    71.88 ns/op   23 B/op   0 allocs/op
BenchmarkSingleMutexInsertParallel-8    3090495   328.8  ns/op  127 B/op   0 allocs/op
BenchmarkIndexLookupParallel-8         18313654    66.00 ns/op    0 B/op   0 allocs/op
BenchmarkSingleMutexLookupParallel-8   11557830   103.9  ns/op    0 B/op   0 allocs/op
```

Sharding is **4.6× faster** on parallel insert and **1.6× faster** on parallel lookup than a single map behind a single mutex, on 8 cores.

Single-threaded, for reference:

```
BenchmarkIndexInsert-8      4182931   342.5 ns/op
BenchmarkIndexLookupHit-8  22718469    50.74 ns/op
```

**A correction worth recording.** The first version of the parallel benchmarks measured 513 ns/op sharded against 525 ns/op single-mutex, which would have said sharding was pointless. The benchmark was wrong: it called `blob.Compute` inside the timed loop, so SHA-256 dominated and both variants were really measuring hashing. Precomputing the IDs outside the timed region exposed the actual 4.6× difference. This is the reason CLAUDE.md R2 insists a number comes with the command that produced it — the first set of numbers was reproducible and meaningless.

## 6. What a reviewer would challenge

**"Why JSON for the pack header? It's slow and bulky."** For the pack header it is a few hundred entries per pack, parsed once per pack read, so the cost is irrelevant next to the blob I/O — and being human-readable makes every future debugging session easier on a format that has to live a long time. The index, which reaches millions of entries, uses fixed-size binary records for exactly the opposite reason. The two choices differ because the constraints differ.

**"256 shards is arbitrary. Why not tune it?"** It is not tuned, it is *derived*: 256 is the number of distinct values of one byte, which makes the selector a bare array index. The measured balance matches Poisson, so there is no skew to fix. If contention ever became the bottleneck on a much larger machine, the change would be a wider prefix (two bytes, 65,536 shards), not a re-tune of 256.

**"Your index is a full in-memory map. What happens at 10 TB?"** It does not fit. At the measured ~72 KiB average chunk size, 10 TB of unique data is ~145M entries at ~90 bytes each — roughly 13 GB of index. That is the honest limit and it is recorded in `docs/OPEN_QUESTIONS.md` Q-003 pending measurement, rather than papered over. The fix would be an on-disk index with a bloom filter in front, which is a different project.

## 7. Deviations from PLAN.md

None.

## 8. What is not done

- The index is written and read whole. There is no incremental update, so a large repository pays a full rewrite on every backup. Acceptable at the scale this is claimed to work at; noted as a real limit.
- No index rebuild-from-packs path yet. The format supports it — that is what `ParseTail` is for — but the command belongs to Phase 8.
- Pack GC (rewriting a pack to drop unreferenced blobs) is not implemented; `Index.Delete` exists in anticipation of it.
