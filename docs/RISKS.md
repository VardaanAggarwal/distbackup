# Risk register

Each risk: what goes wrong, likelihood, blast radius, early warning sign, mitigation.

---

## R-001 — Scope creep past the plan
**Likelihood:** High. This is the most common way a project like this fails.
**Blast radius:** The project ships half-finished, with several components partly built and none of them trustworthy.
**Early warning:** Building a provider before the local path is complete; adding an abstraction "for later"; touching Phase 11 files while on Phase 6.
**Mitigation:** ENGINEERING-RULES.md R5 and the phase table in `PLAN.md`. One phase at a time. gRPC, metrics, and GCP are explicit non-goals, already cut.

---

## R-002 — A misread cloud API parameter is never caught
**Likelihood:** Medium.
**Blast radius:** The EBS/S3 provider is quietly wrong, and nothing in the test suite can reveal it.
**Early warning:** A fact in the code with no source citation or check date; a fake that only implements the happy path.
**Mitigation:** R1 strengthened — read the API reference page itself, cite it with a date. Fakes reproduce documented failure modes, not just success. Every unverifiable claim marked `// UNVERIFIED:` and listed in `OPEN_QUESTIONS.md`. The README states plainly that no real cloud call was ever made.
**Residual:** Real. Accepted knowingly under D-002. This is the honest cost of R7.

---

## R-003 — Buffer aliasing across pipeline stages corrupts data
**Likelihood:** Medium-high if buffer pooling is introduced.
**Blast radius:** Catastrophic and silent. A blob is stored whose bytes do not match its SHA-256 ID; restore produces corrupt data; the round-trip test may still pass if the corruption is self-consistent within one run.
**Early warning:** `sync.Pool` appearing anywhere near the chunk path; a buffer being written to after it was sent on a channel.
**Mitigation:** D-008 forbids buffer reuse across stage boundaries outright. The race detector on every run (R6). A round-trip test that compares against data held independently, not against re-read pipeline output.

---

## R-004 — Concurrency bug that only appears under load
**Likelihood:** Medium.
**Blast radius:** Days lost to debugging; worst case a deadlock that only manifests on a large backup.
**Early warning:** A channel operation not wrapped in a `select` with `<-ctx.Done()`; a receiver closing a channel; goroutine count growing across runs.
**Mitigation:** `go test -race` mandatory (R6). Fixed worker pools only. Sender-closes-channel rule. A goroutine-leak check in the pipeline tests.

---

## R-005 — Crash-safety argument has an uncovered window
**Likelihood:** Medium. The write-ordering argument is easy to state and easy to get subtly wrong.
**Blast radius:** A snapshot references a pack that does not exist; restore fails on data the user believes is backed up.
**Early warning:** The crash test only killing at one convenient point in the run.
**Mitigation:** Mandatory crash test (R6) with SIGKILL at randomised points across many iterations, not a single fixed point. Write ordering packs → index → manifest, each durable before the next. Orphaned packs are safe by construction; dangling references are not, so the ordering is chosen to make only the safe failure possible.

---

## R-006 — EBS pagination token expiry breaks long backups
**Likelihood:** High for any backup exceeding 60 minutes of listing.
**Blast radius:** A long-running backup fails partway and cannot resume from its pagination position.
**Early warning:** Treating the 7-day block-token validity as the binding constraint. It is not — the 60-minute `NextToken` window is.
**Mitigation:** Materialise the full block list up front rather than interleaving listing with fetching over hours. Checkpoint by block index (which is stable) rather than by pagination token (which is not). The fake enforces the 60-minute expiry so tests exercise it.
**Related:** `OPEN_QUESTIONS.md` Q-001 — re-listing to refresh may invalidate outstanding block tokens, so refresh-on-expiry is not a safe fallback.

---

## R-007 — `ListChangedBlocks` empty-page pagination bug
**Likelihood:** High if not explicitly handled. It is documented behaviour that surprises people.
**Blast radius:** Either an infinite loop, or silently backing up nothing and reporting success — the worse of the two.
**Early warning:** Loop termination keyed on "page was empty" rather than "`NextToken` is null."
**Mitigation:** Terminate only on a null `NextToken`. The fake emits empty pages with non-null continuation tokens as a matter of course, so any regression fails a test.

---

## R-008 — `io.ReadCloser` leak on the retry path
**Likelihood:** Medium.
**Blast radius:** Connection exhaustion on a long backup; symptoms appear far from the cause.
**Early warning:** A `GetSnapshotBlock` retry that returns early without draining and closing the previous response body.
**Mitigation:** `GetSnapshotBlockOutput.BlockData` is an `io.ReadCloser` (verified 2026-08-26). Every path that abandons a response drains and closes it. Covered by a fake that fails a test if a body is left unclosed.

---

## R-009 — Index memory blows up on a large corpus
**Likelihood:** Medium at multi-TiB scale; low for anything demonstrable on a laptop.
**Blast radius:** Out-of-memory on a large repository, and a scaling claim in the README that does not survive scrutiny.
**Early warning:** Quoting the ~1 GiB/TiB estimate as though it were measured. It is not (Q-003).
**Mitigation:** Measure it in Phase 13. State the real limit in the README with the corpus it was measured on, and state plainly where it breaks. A documented honest limit is worth more than an unbacked scaling claim (R2).

---

## R-010 — The engine leaks provider concepts
**Likelihood:** Medium. Abstractions erode under deadline pressure.
**Blast radius:** The cloud-agnostic claim — the central design property of this project — stops being true.
**Early warning:** An interface method that exists only because one vendor has it; a core package importing a cloud SDK; a `switch` on provider type in the pipeline.
**Mitigation:** R11 plus an architecture test that fails the build on a cloud SDK import outside a provider package. One shared conformance suite that every provider, including fakes, must pass.
