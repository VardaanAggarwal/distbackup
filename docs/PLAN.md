# distbackup — Build Plan

**Status:** authoritative — this is the source of truth for architecture and scope.
**Written:** 2026-08-26

---

## 1. What this is

A content-addressed, deduplicating backup engine in Go with pluggable providers. The engine knows nothing about any cloud (ENGINEERING-RULES.md R11). Local filesystem is the reference implementation and the default.

**Hard constraint (R7):** no code here is ever run against a real cloud account. Cloud providers are modelled against published API contracts and exercised against fault-injecting fakes. This is stated in the README, not hidden.

---

## 2. Verified facts

Every fact below was checked against official documentation on **2026-08-26**. Re-verify before relying on any of it after ~3 months.

### 2.1 Toolchain

| Fact | Value | Source |
|---|---|---|
| Current Go stable | 1.27.0 (released 2026-08-19); 1.26.7 latest patch | go.dev/doc/devel/release |
| Local toolchain | go1.23.4 darwin/arm64 | `go version` |
| Target in `go.mod` | `go 1.24` | required by `aws-sdk-go-v2/service/ebs` v1.36.8 |

Originally `go 1.23`, to match the installed toolchain. Adding the AWS SDK (D-012) forced 1.24; `GOTOOLCHAIN=auto` downloads it transparently, so the 1.23.4 machine still builds. Still outside the support window — moving to 1.26 is a one-line change. Recorded as D-001.

### 2.2 EBS direct API

| Fact | Value |
|---|---|
| Block size | Fixed **512 KiB** (524288 bytes). Block index = logical offset / 524288, 512 KiB aligned. |
| Block token validity | **7 days** |
| Pagination `NextToken` validity | **60 minutes** ← the binding constraint |
| `MaxResults` | 100–10000. Under 100 is bumped to at least 100. |
| Empty pages | `ListChangedBlocks` **can return an empty page with a non-null `NextToken`**. Must paginate until `NextToken` is null. |
| `ChangedBlock.FirstBlockToken` absent | "the first snapshot does not have the changed block that is on the second snapshot" — i.e. a newly written block |
| Sparse volumes | `ListSnapshotBlocks` returns **only blocks with data written to them** |
| Block index ordering | Unique, numerical order |
| Checksums | Base64 SHA256 per block; service-provided on read, caller-required on write |
| Retryable | 5xx, `ThrottlingException`, `RequestThrottledException` |
| Operations | Read: ListSnapshotBlocks, ListChangedBlocks, GetSnapshotBlock. Write: StartSnapshot, PutSnapshotBlock, CompleteSnapshot |

Pricing (doc's own example region; region-dependent): List\* $0.0006/1,000 requests; GetSnapshotBlock $0.003/1,000 blocks returned; PutSnapshotBlock $0.006/1,000 blocks written.

**Documented ambiguity — unresolved.** The concepts page says block tokens "change ... if you run another ListSnapshotBlocks or ListChangedBlocks request for the same snapshot," while the FAQ says block tokens are valid for 7 days. Whether re-listing *invalidates* outstanding tokens or merely issues different ones is not stated. See `OPEN_QUESTIONS.md` Q-001. Code defensively for both readings: never re-list a snapshot while block tokens from a prior listing are still in flight.

### 2.3 AWS SDK

| Fact | Value |
|---|---|
| Module | `github.com/aws/aws-sdk-go-v2/service/ebs` v1.36.8 (2026-08-25) |
| Constructor | `func NewFromConfig(cfg aws.Config, optFns ...func(*Options)) *Client` |
| Method shape | `func (c *Client) Op(ctx context.Context, params *OpInput, optFns ...func(*Options)) (*OpOutput, error)` |
| `GetSnapshotBlockOutput.BlockData` | **`io.ReadCloser`** — must be drained and closed, including on the retry path |

### 2.4 S3 conditional writes

| Fact | Value |
|---|---|
| Create-if-absent | `If-None-Match: *` on PutObject |
| Key already exists | **412** `PreconditionFailed` |
| Concurrent conflict | **409** `ConditionalRequestConflict` — retryable |
| ETag match | `If-Match: <etag>`; mismatch → 412 |

412 and 409 mean different things and must be classified differently: 412 is "someone else won, and that is fine" (idempotent success for content-addressed writes); 409 is "try again."

---

## 3. Architecture

```
cmd/distbackup/          CLI (stdlib flag, D-010) — the only place that calls os.Exit
internal/
  blob/                  BlobID (SHA-256) and formatting
  errs/                  typed error hierarchy, classification
  retry/                 backoff + jitter                      [from scratch]
  chunker/               FastCDC + Gear rolling hash           [from scratch]
  pack/                  pack file format: writer, reader      [from scratch]
  index/                 sharded dedup index                   [from scratch]
  store/                 ObjectStore interface (core-owned)
    localfs/               reference implementation + default
    s3/                    modelled + fake                     [never run for real]
  source/                Source interfaces (core-owned)
    localfs/               filesystem walker → CDC path
    synth/                 synthetic block device — NOT BUILT, see §6
    ebs/                   modelled + fault-injecting fake     [never run for real]
  repo/                  repository layout, snapshots, verify
  pipeline/              backup/restore orchestration          [from scratch]
```

**Dependency rule (R11):** nothing under `chunker`, `pack`, `index`, `repo`, or `pipeline` imports a cloud SDK. Enforced by an architecture test, not by discipline.

### 3.1 Core-owned interfaces

```go
// store.ObjectStore — where backups land.
type ObjectStore interface {
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    GetRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, error)
    Put(ctx context.Context, key string, data []byte) error
    PutIfAbsent(ctx context.Context, key string, data []byte) (created bool, err error)
    List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    Delete(ctx context.Context, key string) error
}
```

`PutIfAbsent` returning `created bool` rather than an error models S3's `If-None-Match` honestly: 412 is not a failure for content-addressed data, it is "already there." Local implements it with `O_EXCL`.

```go
// source.BlockSource — fixed-size block devices (EBS, synthetic).
type BlockSource interface {
    BlockSize() int64
    Size(ctx context.Context) (int64, error)
    ListBlocks(ctx context.Context, fn func(BlockRef) error) error
    ListChangedBlocks(ctx context.Context, since string, fn func(ChangedBlockRef) error) error
    ReadBlock(ctx context.Context, ref BlockRef, buf []byte) (int, error)
    ID() string
    Close() error
}

// source.FileSource — variable-length files (local filesystem).
type FileSource interface {
    Walk(ctx context.Context, fn func(FileEntry) error) error
    Open(ctx context.Context, path string) (io.ReadCloser, error)
    Root() string
    Close() error
}
```

Two interfaces, not one. A block device has stable fixed-size addressable blocks; a filesystem has variable-length byte streams that need content-defined chunking. Forcing them into one interface would mean the file path carries a fake block index, or the block source fakes a directory walk. Both are lies the rest of the engine would have to work around.

### 3.2 The chunking split

- **Block sources → fixed 512 KiB blocks.** No CDC. The provider already gives stable, aligned, addressable blocks and tells us which ones changed. Running CDC over them would destroy that alignment for no gain, and would mean re-reading unchanged blocks to find boundaries — which for EBS is the expensive operation.
- **File sources → FastCDC.** Files shift under insertion; fixed blocks would re-store an entire file after a one-byte prepend.

Dedup still works across both: everything becomes a blob keyed by SHA-256 of its content.

### 3.3 FastCDC parameters

| Parameter | Value |
|---|---|
| Min | 16 KiB |
| Target (normal) | 64 KiB |
| Max | 256 KiB |
| Normalization | 2 |

Reasoning and rejected alternatives in `DECISIONS.md` D-004. Summary: 64 KiB target keeps index entries per TiB at roughly 16M, which at ~60 bytes/entry is ~1 GiB of index per TiB of unique data — large but tractable and honest. Smaller chunks dedup better and cost more index; this is the trade-off to be able to defend, with a measured chunk-size histogram to back it.

---

## 4. Repository format v1

```
<repo>/
  config                        JSON, format version, chunker params, created-at
  keys/                         (reserved; encryption is a non-goal)
  packs/<aa>/<blobid-hex>       pack files, 2-char fan-out
  index/<shard>.idx             serialised index shards
  snapshots/<snapshot-id>.json  snapshot manifests
```

**Pack layout** (header at the end — D-007):

```
[blob data][blob data]...[header JSON][uint32 header length][8-byte magic "DBPACK01"]
```

Header at the tail permits a single streaming forward pass with no buffering of the whole pack, and index-free recovery via one small ranged GET of the tail. Cost: readers seek to the end, one extra range request when the index is unavailable.

**Write ordering for crash safety:** packs → index → snapshot manifest, each fsync'd (local) or confirmed (object store) before the next begins. A snapshot manifest is written last and atomically, so a snapshot never references a pack that does not exist. The reverse — an orphaned pack with no snapshot — is safe and is what `gc` reclaims.

**Forward compatibility:** `config` carries `format_version`. A reader refuses a `format_version` it does not know rather than guessing. Unknown fields in snapshot manifests are preserved on rewrite.

---

## 5. Concurrency model

| Stage | Workers | Channel capacity | Reasoning |
|---|---|---|---|
| Source read | `min(32, 4×NumCPU)` | 64 | I/O-bound; for cloud sources bounded by provider throttling, not CPU |
| Chunk/hash | `NumCPU` | 128 | CPU-bound (SHA-256 dominates) |
| Pack assembly | 1 | 32 | Single writer per pack keeps the format append-only and the ordering trivially correct |
| Upload | `min(16, 2×NumCPU)` | 32 | Bounded to avoid unbounded memory in flight |

Fixed pools only, never one goroutine per chunk. The sender closes the channel. Every channel operation sits in a `select` with `<-ctx.Done()`. Buffer reuse via `sync.Pool` is deliberately **not** used across stage boundaries — aliasing a reused buffer into a queued chunk is the single most likely data-corruption bug in this design (see `RISKS.md` R-003).

---

## 6. Phase plan

Each phase: acceptance criteria must pass before the next begins (R6). `go test -race ./...` and `golangci-lint run` clean at every phase boundary.

| # | Phase | Status | Notes |
|---|---|---|---|
| 0 | Scaffolding, errors, retry, arch test | **done** | Arch test verified to fail on a planted cloud import |
| 1 | FastCDC chunker | **done** | Boundary-shift 100% (bar: 95%) |
| 2 | Pack format | **done** | Truncation, bad magic, absurd length, tail recovery all covered |
| 3 | Sharded index | **done** | Concurrent-dedup test verified to fail on broken locking |
| 4 | ObjectStore + localfs + conformance suite | **done** | Suite runs with and without fsync |
| 5 | Repository, snapshots, versioning | **done** | Index rebuild from pack tails works |
| 6 | Backup pipeline | **done** | Fixed pools, no goroutine leaks |
| 7 | Restore | **done** | Round-trip byte-identical |
| 8 | Verify + GC | **done** | Crash test SIGKILLs a real subprocess |
| 9 | CLI | **done** | `--dry-run`, `--max-bytes`, cost line |
| 10 | Sources | **partial** | localfs walker done; **synthetic block device not built** |
| 11 | EBS provider + fake | **done** | Plus an SDK adapter, compile-checked against v1.36.8 |
| 12 | S3 store + fake | **done** | Passes the same conformance suite; 412 vs 409 distinguished |
| 13 | Benchmarks + README | **done** | Every README number carries its command |

### What was not built

- **The synthetic block device source (Phase 10).** `source.BlockSource` has exactly one implementation, the EBS client. The interface is therefore validated by one provider rather than two, which is weaker than the ObjectStore side where localfs and S3 both pass a shared suite. A synthetic device would also let the block-backup path run end to end; as it stands, `BackupBlocks` does not exist — only `BackupFiles` does.
- **The block backup pipeline.** The `BlockSource` interface, the EBS client, and the `KindBlocks` snapshot shape are all present and tested, but nothing wires a block source through the pipeline into a repository. This is the largest honest gap in the project.
- **An S3 SDK adapter.** The EBS package has one (D-012); the S3 package does not. The pattern is demonstrated once rather than twice, deliberately, to avoid a second dependency for code that can never execute.

GCP providers are **out of scope for v1** (R5: one cloud provider complete before a second). The interfaces make it additive; the README says so plainly rather than implying GCP support exists.

---

## 7. Benchmarks to run

All numbers recorded with command, hardware, and date, or marked `TODO: not yet measured` (R2).

```bash
go test -bench=BenchmarkChunker -benchmem ./internal/chunker/
go test -bench=BenchmarkIndexInsert -benchmem ./internal/index/
go test -bench=BenchmarkPackWrite -benchmem ./internal/pack/
go test -run=TestChunkSizeDistribution -v ./internal/chunker/   # histogram
go test -bench=BenchmarkBackupPipeline -benchmem ./internal/pipeline/
```

---

## 8. What this project does not do

Stated here so the README cannot overclaim: no client-side encryption, no compression, no web UI, no distributed control plane, no cross-region replication, no multi-writer coordination, no gRPC/HTTP service, no metrics instrumentation, no GCP providers in v1, and **no execution against any real cloud account**.
