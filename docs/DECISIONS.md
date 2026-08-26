# Decision log

Append-only. One entry per meaningful decision (CLAUDE.md R9).

---

## D-001: Target Go 1.24
**Date:** 2026-08-26 (revised same day)
**Decision:** `go.mod` declares `go 1.24`.
**Originally:** `go 1.23`, chosen to match the installed toolchain (go1.23.4) on the reasoning that declaring a version above it breaks the build for no benefit.
**Why it changed:** `aws-sdk-go-v2/service/ebs` v1.36.8 requires 1.24, so adding it (D-012) forced the bump. Go's `GOTOOLCHAIN=auto` downloads go1.24.0 transparently, so the build works on the 1.23.4 machine anyway — which was the original concern, and it turned out not to apply.
**Trade-off accepted:** A fresh clone downloads a toolchain on first build. 1.24 is still outside the support window (1.25/1.26/1.27 have shipped), so there are no security backports; moving to 1.26 is a one-line change.
**Note:** `golangci-lint` had to be reinstalled under 1.24 — a linter built with an older toolchain refuses to analyse a newer language version.
**Source:** https://go.dev/doc/devel/release, checked 2026-08-26.

---

## D-002: Never execute against a real cloud account
**Date:** 2026-08-26
**Decision:** Cloud provider clients are modelled against published API contracts and exercised only against local fault-injecting fakes. No real API call, no cloud CLI, no real credentials, ever.
**Alternatives considered:** A capped real run against an 8 GiB volume with `--dry-run` and `--max-blocks` guardrails (the original R7).
**Rationale:** Vardaan's explicit instruction. Removes all spend and real-account risk from a portfolio project. A faithful fake that reproduces documented failure modes demonstrates more engineering judgment than a happy-path live call would.
**Trade-off accepted:** No integration validation. If a documented parameter is misread, no test will catch it — which is why R1 was strengthened to require reading the actual API reference page, and why the fakes are on the from-scratch list. The README must state this plainly; claiming or implying real-cloud validation would be fabrication under R2.
**Source:** CLAUDE.md R7 as amended 2026-08-26.

---

## D-003: Two source interfaces (BlockSource, FileSource), not one
**Date:** 2026-08-26
**Decision:** `source.BlockSource` for fixed-size block devices; `source.FileSource` for variable-length file trees.
**Alternatives considered:** A single unified `Source` interface yielding opaque byte ranges.
**Rationale:** A block device has stable, fixed-size, addressable blocks and a native changed-block query. A file tree has variable-length streams with no stable addressing under insertion. Unifying them forces one side to lie — either a file path carries a synthetic block index, or a block device fakes a directory walk — and the engine then works around the lie at every call site.
**Trade-off accepted:** Two backup entry points in the pipeline instead of one. Accepted because the two paths genuinely differ (fixed blocks vs CDC).
**Source:** Design judgement; the split follows the block-vs-file distinction in the EBS direct API contract.

---

## D-004: FastCDC at min 16 KiB / target 64 KiB / max 256 KiB, normalization 2
**Date:** 2026-08-26
**Decision:** As stated.
**Alternatives considered:** (a) 8/32/128 KiB — better dedup, ~2× the index entries. (b) 32/128/512 KiB — half the index, measurably worse dedup on small edits. (c) Normalization level 1 or 3.
**Rationale:** At a 64 KiB average, 1 TiB of unique data is ~16M chunks; at roughly 60 bytes per index entry that is ~1 GiB of index per TiB. That is large but tractable on a laptop and honest to state. Normalization 2 is FastCDC's recommended default: two masks (stricter below target, looser above) pull the size distribution toward the target instead of leaving it exponential, which is what a single mask produces.
**Trade-off accepted:** Dedup ratio is worse than an 8 KiB average would give. The chunk-size histogram must be measured (not assumed) to back this up.
**Source:** Xia et al., FastCDC (USENIX ATC '16), normalized chunking. Index-size arithmetic is ours and must be confirmed by measurement under R2.

---

## D-005: PutIfAbsent returns (created bool, err error)
**Date:** 2026-08-26
**Decision:** The conditional-write primitive returns whether it created the object, rather than treating "already exists" as an error.
**Alternatives considered:** Return a typed `ErrAlreadyExists` and make every caller classify it.
**Rationale:** For content-addressed data, "this blob already exists" is the *success* case for dedup, not a failure. S3 signals it as HTTP 412 and every caller would immediately translate that back into "fine, carry on." Encoding it in the return type puts the meaning where the reader sees it.
**Trade-off accepted:** Diverges from the SDK's error-shaped signal, so the S3 adapter must translate 412 → `(false, nil)`. That translation is exactly the provider's job under R11.
**Source:** S3 conditional-write docs, checked 2026-08-26: `If-None-Match: *` → 412 PreconditionFailed if the key exists; 409 ConditionalRequestConflict on a concurrent race (retryable).

---

## D-006: 412 and 409 are classified differently
**Date:** 2026-08-26
**Decision:** S3 412 `PreconditionFailed` → `(created=false, nil)`. S3 409 `ConditionalRequestConflict` → retryable error.
**Alternatives considered:** Treat both as "already exists."
**Rationale:** They mean different things. 412 is a settled outcome: the key exists, another writer won, and for content-addressed data the bytes are the same. 409 is unsettled: a concurrent operation was in flight and the request must be retried. Collapsing them would silently skip a blob that was never actually written.
**Trade-off accepted:** None material; it is a two-line distinction that prevents a data-loss class of bug.
**Source:** AWS S3 conditional-request docs, checked 2026-08-26.

---

## D-007: Pack header at the end of the file
**Date:** 2026-08-26
**Decision:** Header written after all blobs, followed by uint32 header length and 8-byte magic.
**Alternatives considered:** Header at the start (requires two passes or buffering the whole pack in memory).
**Rationale:** Single streaming forward pass with bounded memory; enables index-free recovery via one small ranged GET of the tail.
**Trade-off accepted:** Readers must seek to the end, costing one extra range request when the index is unavailable.
**Source:** restic's repository design uses the same approach.

---

## D-008: No buffer reuse across pipeline stage boundaries
**Date:** 2026-08-26
**Decision:** Buffers are not pooled or reused once handed to a downstream stage via a channel.
**Alternatives considered:** `sync.Pool` for chunk buffers to cut allocation pressure.
**Rationale:** Handing a pooled buffer to another goroutine and then reusing it is the single most likely data-corruption bug in this design, and it is exactly the kind that passes tests and corrupts backups. It would silently produce a blob whose SHA-256 does not match its bytes.
**Trade-off accepted:** Higher allocation rate and GC pressure. If benchmarks show this is the bottleneck, the fix is per-worker buffers with an explicit copy at the hand-off, recorded as a new decision — not pooling.
**Source:** Design judgement. See `RISKS.md` R-003.

---

## D-009: GCP providers deferred out of v1
**Date:** 2026-08-26
**Decision:** v1 ships local filesystem providers plus modelled AWS (EBS source, S3 store). No GCP.
**Alternatives considered:** Ship GCS and persistent-disk providers alongside AWS.
**Rationale:** CLAUDE.md R5: provider count is scope, and one cloud provider must be complete before a second starts. The provider abstraction (R11) is what demonstrates cloud-agnosticism; a second half-finished provider demonstrates less than one complete one plus a clean interface.
**Trade-off accepted:** The project is cloud-agnostic by construction but only demonstrates one cloud. The README must say this explicitly rather than implying GCP support exists.
**Source:** CLAUDE.md R5.

---

## D-010: CLI uses the standard library `flag`, not cobra
**Date:** 2026-08-26
**Decision:** Subcommand dispatch is a `switch` over `os.Args[1]` with a `flag.FlagSet` per command.
**Alternatives considered:** cobra, which CLAUDE.md R3 explicitly permits.
**Rationale:** Six commands do not need a framework, and staying on the standard library keeps the whole project at **zero third-party dependencies**. That is not cosmetic: R7 requires `go test ./...` to pass with the machine offline, and every dependency is a way for that to stop being true. It also means a reviewer can read the entire program without knowing anyone else's API.
**Trade-off accepted:** No generated completions, no nested subcommands, and help text is hand-written. All cheap at this size.
**Related:** `golang.org/x/sync/errgroup` was added and then removed for the same reason — it silently pushed the `go` directive from 1.23 to 1.25 and pulled a new toolchain. The ~30 lines of coordination it provided are in `internal/pipeline/group.go`, which R3 wanted from scratch anyway.

---

## D-011: An interrupted backup's packs are adopted by the next run
**Date:** 2026-08-26
**Decision:** Documented and tested as intended behaviour, not tidied away.
**Alternatives considered:** Treating post-crash packs as garbage to be collected before the next backup.
**Rationale:** Found by a test that expected the opposite. A crashed run leaves packs but no index and no manifest. The next `repo.Open` finds no index and rebuilds one from the pack tails, so those blobs are already known and the new backup deduplicates against them — the interrupted run's work is reused rather than repeated.
It falls out of two decisions made for other reasons: content addressing (a blob's identity does not depend on which run wrote it) and an index that is a rebuildable cache rather than a source of truth.
**Trade-off accepted:** Packs from a crashed run whose source is never backed up again linger until `gc`. That is the case `gc` exists for.
**Source:** `internal/e2e.TestCrashedBackupWorkIsReusedByNextRun` and `TestGCReclaimsGenuineOrphans`.


---

## D-012: Keep an AWS SDK adapter that can never be executed
**Date:** 2026-08-26
**Decision:** `internal/source/ebs/awsclient.go` binds the package's `API` interface to `aws-sdk-go-v2/service/ebs` v1.36.8, despite R7 guaranteeing it will never run and the CLI offering no way to construct it.
**Alternatives considered:** Omit it entirely and keep the project at zero third-party dependencies, describing the SDK binding in prose instead.
**Rationale:** It is the only correctness evidence available under R7 that does not depend on my own reading of the documentation. Everything else in the package is checked against `Fake`, and a fake can only confirm that the client agrees with the same understanding that produced it. The adapter is type-checked by the compiler against the real SDK, which caught three things prose would not have: every index is `*int32` (not int64), almost every field is a pointer so a missing value is nil rather than zero, and `GetSnapshotBlockOutput.BlockData` really is an `io.ReadCloser`.
**Trade-off accepted:** Five transitive dependencies, a `go` directive bump to 1.24 (D-001), and ~200 lines that no test executes. The guarantee is narrow and must be described as such: **the types line up; nothing is proven about runtime behaviour.**
**Source:** `go doc` against module v1.36.8, 2026-08-26.

---

## D-013: Serialise access to an injected retry jitter source
**Date:** 2026-08-26
**Decision:** `retry.Policy.Rand`, when non-nil, is accessed under a package-level `jitterMu`.
**Alternatives considered:** (a) Document that an injected `Rand` is single-goroutine only. (b) Put a mutex in `Policy`.
**Rationale:** Found by the race detector: 32 goroutines retrying concurrently through one `Policy` value sharing one `*rand.Rand`, which is not safe for concurrent use. Option (a) makes a value type unsafe in a way that is invisible at the call site — a `Policy` is copied and passed around freely, so "do not share it" is not a constraint a caller can reasonably honour. Option (b) does not work: copying a struct containing a `sync.Mutex` copies the lock and defeats it, and `Policy` is copied by value everywhere.
**Trade-off accepted:** A global lock on the jitter path. Irrelevant in practice — jitter is computed at most once per retry, and the nil-Rand default path (production) uses `math/rand`'s package functions, which are already mutex-protected and take no lock here.
**Source:** `go test -race ./internal/store/s3/` failing on `PutIfAbsentIsAtomicUnderRace`.
