# Decision log

Append-only. One entry per meaningful decision (CLAUDE.md R9).

---

## D-001: Target Go 1.23 despite 1.27 being current
**Date:** 2026-08-26
**Decision:** `go.mod` declares `go 1.23`.
**Alternatives considered:** Target 1.26/1.27 (current stable line).
**Rationale:** The installed toolchain is go1.23.4. Declaring a version above the installed toolchain breaks the build for no benefit; nothing in the design needs a post-1.23 language or stdlib feature.
**Trade-off accepted:** 1.23 is outside Go's support window (supported until two newer majors exist; 1.25/1.26/1.27 all shipped). No security backports. Upgrading is a one-line change.
**Source:** https://go.dev/doc/devel/release, checked 2026-08-26. Local `go version`.

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
