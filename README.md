# distbackup

A content-addressed, deduplicating backup engine in Go, with pluggable providers.

```bash
distbackup init    --repo /backups/repo
distbackup backup  --repo /backups/repo --source ~/documents
distbackup verify  --repo /backups/repo --full
distbackup restore --repo /backups/repo --snapshot latest --target /tmp/restored
```

---

## Read this first

**No code in this project has ever run against a real cloud account.** Not AWS, not GCP, not anything billable or free.

The AWS providers here — an EBS direct API block source and an S3 object store — were written against the published API references (checked 2026-08-26, citations inline in the code) and are exercised against fault-injecting fakes that reproduce the documented behaviour, including the parts that are easy to get wrong. The CLI deliberately offers no flag to select a cloud backend.

What that buys and what it costs:

- **It buys** a test suite that runs entirely offline — `go test -race ./...` completes in **9 s** once compiled (Apple M1) — deterministically, with no credentials and no spend.
- **It costs** integration validation. If I misread a documented parameter, no test here would catch it. The one compile-time check available is `internal/source/ebs/awsclient.go`, which is type-checked by the Go compiler against `aws-sdk-go-v2/service/ebs` v1.36.8 — that proves the *shapes* line up and nothing about runtime behaviour.

Every number below was measured by a command that is printed next to it. Nothing here is estimated, extrapolated, or remembered.

---

## What it does

Splits data into content-defined chunks, addresses each by the SHA-256 of its bytes, and stores only chunks it has never seen before. Backing up a file after a one-byte edit re-stores roughly one chunk, not the file.

```
source ──► FastCDC chunker ──► SHA-256 ──► dedup index ──► pack files ──► object store
```

**Two source shapes, deliberately not unified:**

| | Addressing | Chunking |
|---|---|---|
| File trees | variable-length streams, by path | FastCDC (content-defined) |
| Block devices | fixed 512 KiB blocks, by index | none — the blocks are already stable |

Forcing these into one interface would mean a file path carrying a fake block index, or a block device faking a directory walk. See `docs/DECISIONS.md` D-003.

---

## Measured results

Apple M1, `darwin/arm64`, Go 1.24.0, 2026-08-26. Reproduce with the commands shown.

### Content-defined chunking works

```bash
go test -race -run TestBoundaryShift -v ./internal/chunker/
```

```
original chunks: 172, shifted chunks: 172, matched boundaries: 172 (100.00%)
fixed-size chunking scores 0.00% on the same measurement, as expected
```

Insert one byte at the front of a 12 MiB buffer: **100%** of chunk boundaries land at the same content. The mandatory bar is 95%. The second line is a control — the identical measurement applied to fixed-size chunking, which scores 0%, proving the test discriminates rather than being trivially satisfiable.

End to end, on an 8 MiB file with one byte prepended:

```
re-stored 83,599 of 8,388,609 bytes (1.00%), 1 new blob of 113
```

### Chunk size distribution

```bash
go test -run TestChunkSizeDistribution -v ./internal/chunker/
```

```
mean    74,291 bytes (72.55 KiB), target 64 KiB
p50     74,016
p99    131,495
max    164,258
forced cuts at MaxSize: 0 (0.00%)
```

The mean sits ~13% above target because cut-point skipping suppresses boundaries in the first 16 KiB of every chunk. Zero forced cuts is why the boundary-shift rate is 100% on this corpus: every boundary is content-defined, so every one resynchronises.

### Throughput

```bash
go test -bench=. -benchmem -run='^$' ./internal/chunker/ ./internal/pack/ ./internal/blob/ ./internal/pipeline/
```

| Benchmark | Throughput | Allocations |
|---|---:|---:|
| Full backup pipeline (32 MiB) | **640.6 MB/s** | 2,436 allocs |
| Chunker (chunk + copy) | 1,716.1 MB/s | 226 allocs |
| Boundary scan only | 2,001.4 MB/s | 0 allocs |
| SHA-256 of 64 KiB | 2,397.8 MB/s | 0 allocs |
| Pack write | 1,097.5 MB/s | 234 allocs |
| Pack read one blob | 2,114.4 MB/s | 1 alloc |

### Sharding the index earns its keep

```bash
go test -bench=Parallel -benchmem -run='^$' ./internal/index/
```

| | Sharded (256) | Single mutex | Speedup |
|---|---:|---:|---:|
| Parallel insert | 79.0 ns/op | 317.4 ns/op | **4.0×** |
| Parallel lookup | 68.5 ns/op | 123.2 ns/op | **1.8×** |

The shard key is the first byte of the blob ID — no hash function, because a SHA-256 digest *is* a uniform hash. Measured balance over 102,400 entries: mean 400.0, stddev **20.3** against a Poisson prediction of √400 = 20.0.

> An earlier version of this benchmark reported 513 vs 525 ns/op, which would have said sharding was pointless. The benchmark was wrong — it hashed keys inside the timed loop, so both variants were really measuring SHA-256. This is why `CLAUDE.md` R2 requires a number to come with the command that produced it.

---

## Crash safety

A backup writes in exactly this order, each step durable before the next:

```
1. packs      the data
2. index      a rebuildable cache
3. snapshot   the manifest that references the data
```

Every reachable crash state is harmless:

| Crash during | Result |
|---|---|
| packs | Orphaned packs. Wasted space, reclaimed by `gc`. |
| index | The index is a cache; the next open rebuilds it from pack tails. |
| snapshot | No snapshot, so the run did not happen. Packs are orphans, as above. |

What the ordering makes **impossible** is a snapshot referencing a pack that does not exist — the one failure that loses data a user believes is backed up.

Tested by SIGKILLing a real subprocess at randomised points across 12 iterations, then asserting the repository still verifies:

```bash
go test -race -run TestCrash ./internal/e2e/
```

A pleasant consequence, found by a test that expected the opposite: **an interrupted backup's work is not wasted.** The next run finds no index, rebuilds one from the pack tails, and deduplicates against the orphaned data instead of rewriting it (`docs/DECISIONS.md` D-011).

---

## Architecture

```
cmd/distbackup/          CLI — the only package that calls os.Exit
internal/
  blob/                  content address (SHA-256), a comparable [32]byte
  errs/                  typed error taxonomy; retry decisions come from Kind
  retry/                 exponential backoff, full jitter          [from scratch]
  chunker/               FastCDC + Gear rolling hash               [from scratch]
  pack/                  pack format, header at the tail           [from scratch]
  index/                 256-way sharded dedup index               [from scratch]
  pipeline/              backup/restore orchestration              [from scratch]
  repo/                  layout, snapshots, write ordering, verify, gc
  source/                BlockSource + FileSource (core-owned interfaces)
    localfs/ ebs/          directory walker · EBS + fake
  store/                 ObjectStore (core-owned interface)
    localfs/ s3/           filesystem · S3 + fake
    storetest/             one conformance suite every backend must pass
  arch/                  architecture tests that enforce the layering
  e2e/                   subprocess tests, including the crash test
```

**The core never learns which provider it is talking to.** No package under `chunker`, `pack`, `index`, `pipeline`, or `repo` may import a cloud SDK. That is not a convention — `internal/arch` parses the import graph and fails the build. Planting `import _ "github.com/aws/aws-sdk-go-v2/service/ebs"` in `internal/index` produces:

```
R11 violation: internal/index imports "github.com/aws/aws-sdk-go-v2/service/ebs"
```

Both object stores pass the same conformance suite, assertion for assertion. That is what makes `ObjectStore` an abstraction rather than an aspiration.

---

## Details worth knowing

**Pack header at the end of the file.** A pack is `[blobs][header JSON][uint32 length][magic]`. Writing forward in one pass keeps memory bounded, and a lost index is recoverable with one small ranged read per pack instead of re-downloading everything. The cost is that a reader must seek to the end.

**`PutIfAbsent` returns `(created bool, err error)`.** For content-addressed data, "this blob already exists" is the success case for deduplication, not a failure. S3 reports it as HTTP 412; encoding it in the return type puts the meaning where the reader sees it.

**412 and 409 are not the same.** 412 `PreconditionFailed` means the key exists — settled, and for content-addressed data the bytes are already the right ones. 409 `ConditionalRequestConflict` means a write was in flight and the outcome is unknown, so it must be retried. Collapsing them would report a blob as stored when it may never have been written.

**No buffer reuse across pipeline stages.** `sync.Pool` would cut the allocation rate, and handing a pooled buffer to another goroutine and then overwriting it is the single most likely way to silently store a blob whose bytes do not match its SHA-256. The measured cost of not pooling is ~15% of chunker throughput.

**The Gear table is generated, not hard-coded.** 256 literal constants are 256 numbers nobody can check. It is derived from SplitMix64 with a fixed seed, and pinned by a golden checksum test — changing it changes every boundary and would silently destroy dedup against every existing repository.

---

## Honest limitations

- **Never run against a real cloud.** Stated at the top; restated here because it is the most important caveat in this document.
- **No GCP providers.** The interfaces make them additive, and none are written. The project is cloud-agnostic by construction but demonstrates one cloud.
- **The index is fully in memory.** At the measured ~72.5 KiB average chunk size, 1 TiB of unique data is ~14.8M entries. Per-entry memory has **not been measured** (`docs/OPEN_QUESTIONS.md` Q-003), so no GB-per-TB figure is quoted. It will not survive multi-TB repositories, and the fix — an on-disk index behind a bloom filter — is a different project.
- **The index is rewritten whole on every backup.** No incremental update.
- **Cloud concurrency defaults are reasoned, not tuned.** Nothing here has observed real EBS latency or real throttling thresholds (Q-002).
- **One documented ambiguity is unresolved.** Whether re-listing an EBS snapshot invalidates outstanding block tokens (Q-001). The docs say both "valid for seven days" and "they change if you run another list request". The client assumes the stricter reading; only a real call could settle it, and that is out of scope.
- **Symlinks, devices, and sockets are skipped**, not stored as links.
- **No encryption, no compression, no service layer, no metrics.** Explicit non-goals.

---

## Development

```bash
go test -race ./...          # 9s warm, no network required
golangci-lint run ./...
go build -o distbackup ./cmd/distbackup
```

Zero third-party dependencies outside the AWS SDK, which only `internal/source/ebs/awsclient.go` imports. The retry logic, the worker-pool coordination, the chunker, the pack format, and the index are all written here.

Design documents live in `docs/`: `PLAN.md` (architecture and verified facts), `DECISIONS.md` (every decision with the alternative rejected), `RISKS.md`, `OPEN_QUESTIONS.md`, and per-phase summaries.
