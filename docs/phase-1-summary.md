# Phase 1 — FastCDC chunker

## 1. What was built

`internal/chunker` splits a byte stream into content-defined chunks using FastCDC over a Gear rolling hash. Two files:

- `gear.go` — the 256-entry Gear substitution table, generated deterministically at init from SplitMix64 with a fixed seed.
- `chunker.go` — configuration and validation, mask derivation, the streaming `Next()` API, and the boundary scan.

## 2. Key decisions

**Generated the Gear table instead of embedding 256 literals.** A literal table is 256 numbers nobody reviewing the code can check; one typo would degrade boundary quality invisibly. Generating from a named algorithm and a fixed seed makes the provenance twenty readable lines. *Rejected:* `math/rand`, whose output is explicitly not stable across Go releases — the table would change on a toolchain upgrade and silently stop deduplicating against every prior backup.

**Derived the masks from `AvgSize` rather than copying the paper's constants.** The FastCDC paper's masks are tuned for an 8 KiB average; this project targets 64 KiB (D-004). Rescaling them by hand produces numbers with no verifiable provenance. Deriving them puts the relationship between target size and mask width in code, and the resulting distribution is *measured* rather than asserted.

**Mask bits live in positions 16–63, not 0–63.** With `h = (h<<1) + G[b]`, bit *k* is influenced by roughly the last *k+1* bytes. Bit 0 depends on the last byte alone. Including low bits would let a single byte dominate the boundary decision. Starting at bit 16 gives every mask bit at least 16 bytes of history.

**`Chunk.Data` is freshly allocated per chunk.** *Rejected:* returning a window into the internal buffer, which is faster and is the single most likely source of silent corruption here — the pipeline hands chunks to other goroutines through a channel and the buffer would be overwritten underneath them (D-008, R-003).

**`fill()` returns nothing; read errors go to a sticky field.** A reader may return data alongside an error. Returning immediately would drop the buffered tail. The error surfaces once the buffer drains, so a caller gets every legitimately-read chunk and *then* the failure.

## 3. How it works

`Next()` tops up the buffer so at least `MaxSize` bytes are available, then calls `boundary()`, which is the whole algorithm:

1. **Skip to `MinSize`.** No boundary may be emitted before it, so hashing those bytes is wasted work ("cut-point skipping"). Costs a little boundary quality — a natural cut inside the skipped region is suppressed — and buys a real speedup, since `MinSize` is a quarter of the average chunk.
2. **`MinSize` → `AvgSize`: strict mask** (18 bits set). A boundary needs 18 specific hash bits to be zero, so `P ≈ 2⁻¹⁸`. Cuts are rare here.
3. **`AvgSize` → `MaxSize`: loose mask** (14 bits set). `P ≈ 2⁻¹⁴`. Cuts are likely, so most chunks land shortly after the target.
4. **At `MaxSize`: forced cut.** Bounds memory and read amplification. These are the only boundaries that are *not* content-defined, and the only ones that fail to resynchronise after an insertion.

That two-mask structure is normalized chunking, and it is the most valuable idea in FastCDC. A single mask gives a geometric distribution: many tiny chunks (which bloat the index) and a long tail at the maximum (which dedups poorly). Two masks squeeze the distribution toward the target from both sides.

## 4. Test output

```
$ go test -race ./...
ok  github.com/vardaanaggarwal/distbackup/internal/arch     1.219s
ok  github.com/vardaanaggarwal/distbackup/internal/blob     (cached)
ok  github.com/vardaanaggarwal/distbackup/internal/chunker  (cached)
ok  github.com/vardaanaggarwal/distbackup/internal/errs     (cached)
ok  github.com/vardaanaggarwal/distbackup/internal/retry    (cached)
TEST EXIT: 0

$ golangci-lint run ./...
LINT EXIT: 0
```

Mandatory boundary-shift test (CLAUDE.md R6):

```
=== RUN   TestBoundaryShift
    chunker_test.go:80: original chunks: 172, shifted chunks: 172, matched boundaries: 172 (100.00%)
--- PASS: TestBoundaryShift (0.60s)
=== RUN   TestBoundaryShiftMeasurementDetectsFixedChunking
    chunker_test.go:119: fixed-size chunking scores 0.00% on the same measurement, as expected
--- PASS
=== RUN   TestBoundaryShiftMidStream
    chunker_test.go:173: after mid-stream insertion: 56/56 boundaries realigned (100.00%)
--- PASS
```

Required ≥95%; measured 100%. The second test is the control: it runs the identical measurement against fixed-size chunking and confirms it scores 0%, which proves the measurement discriminates rather than being trivially satisfiable.

## 5. Measurements

Apple M1, darwin/arm64, Go 1.23.4, 2026-08-26.

```
$ go test -bench='BenchmarkChunker|BenchmarkBoundaryScan' -benchmem -run='^$' ./internal/chunker/
BenchmarkChunker-8        120   9787267 ns/op  1714.19 MB/s  18261181 B/op  226 allocs/op
BenchmarkBoundaryScan-8  2301    521824 ns/op  2009.44 MB/s         0 B/op    0 allocs/op
```

Chunking throughput **1714 MB/s** end to end; the boundary scan alone runs at **2009 MB/s** with zero allocations. The ~15% difference is the per-chunk allocation and copy that D-008 deliberately accepts.

Chunk size distribution, 64 MiB of deterministic pseudo-random input, 903 chunks:

```
mean    74291 bytes (72.55 KiB), target 64 KiB
min     16463
p10     42595
p50     74016
p90     99573
p99    131495
max    164258
forced cuts at MaxSize: 0 (0.00%)
```

The mean sits ~13% above the 64 KiB target. That is expected and worth stating plainly: cut-point skipping suppresses every boundary in the first 16 KiB of each chunk, which shifts the whole distribution upward. Zero forced cuts at `MaxSize` is why the boundary-shift rate is 100% — every boundary in this corpus is content-defined, so every one of them resynchronises.

## 6. What a reviewer would challenge

**"Random data is the easy case. What happens on real files?"** Fair, and the honest answer is that this corpus is pseudo-random, which is the *best* case for CDC because every window is well-mixed. Highly repetitive data (long zero runs, for example) produces a degenerate hash that rarely triggers either mask, pushing chunks to `MaxSize` and producing forced, non-content-defined cuts. The measured 0% forced-cut rate would rise. The defensible claim is "100% on this corpus," not "100% always."

**"Your masks aren't the paper's masks. How do you know they're good?"** I don't know it from authority — I know it from the measured distribution and the boundary-shift rate. The mask derivation is visible in `spreadMask` and the reasoning for bits 16–63 follows from the Gear recurrence. The evidence is `TestChunkSizeDistribution`, which reports the real histogram, and `TestStrictMaskIsStricterThanLoose`, which catches an inverted normalization.

**"Why allocate per chunk when you have a perfectly good buffer?"** Because the pipeline sends chunks across a channel to other goroutines. Handing over a slice into a buffer I am about to overwrite produces a blob whose stored bytes do not match its SHA-256 — corruption that is silent, passes a naive round-trip test, and is only discovered when a restore is needed. Measured cost is ~15% of chunking throughput, and chunking is not the pipeline's bottleneck; SHA-256 and I/O are. If it ever is, the fix is per-worker buffers with an explicit copy at the hand-off, not pooling.

## 7. Deviations from PLAN.md

None.

## 8. What is not done

- The Gear table checksum is pinned, but there is no format-version field yet that would let a future chunker parameter change be detected at read time. That belongs to Phase 5 (repository config).
- No test yet on pathological input (all-zeros, highly repetitive). Worth adding when the pipeline exists so the effect on dedup ratio can be measured end to end rather than in isolation.
- `Split()` holds all chunks in memory. It is a test and small-input convenience; the pipeline uses `Next()` directly.
