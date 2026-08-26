# CLAUDE.md — Standing rules for this repository

> Place this file at the repository root. Claude Code reads it automatically at the start of every session. These rules apply to **all** work in this repo, in every session, in both planning and build phases. They are not overridable by convenience.

---

## What this project is

`distbackup` — a content-addressed, deduplicating backup engine written in Go, with **pluggable providers**.

It is deliberately **not** an AWS project. The engine — chunking, dedup, the pack format, the index, crash safety, the pipeline — knows nothing about any cloud. Providers plug in behind interfaces:

- **Block sources** (what gets backed up): a local filesystem walker, a synthetic block device, an AWS EBS snapshot reader, a GCP persistent-disk reader.
- **Object stores** (where backups land): a local filesystem store, S3, GCS.

The local filesystem provider is the reference implementation and the default. Cloud providers are modelled against their published API contracts and exercised against fakes — see R7, which is absolute.

## Who it is for, and why that shapes everything

This is a portfolio project for **Vardaan Aggarwal**, a backend engineer with ~3 years of experience (Go, Python, GCP, event-driven microservices), applying to **Rubrik's Cloud Native Protection team** in Bangalore. He will be interviewed on this code, line by line, by senior storage and distributed-systems engineers.

Three consequences you must internalise:

1. **He must be able to explain every line.** Clever code that he cannot defend is worse than straightforward code that he can. When there is a choice between an elegant trick and an obvious implementation, choose the obvious one and note the trick in a comment.
2. **The reasoning is the deliverable, not just the binary.** Comments, decision records, and phase summaries are first-class output. They are his study material.
3. **Honest limitations beat impressive claims.** A documented "this breaks at 100 TB and here is why" is worth more in an interview than a system that pretends to scale. Never write a README claim the code does not support.

---

## Standing rules

### R1 — Verify, never assume, external facts

Your training data has a cutoff. Cloud APIs, SDK signatures, pricing, and service limits change.

Before writing code that depends on an external fact — an API parameter, a limit, a price, a library signature — **verify it against current official documentation**. Cite the source, with the date you checked it, in a comment or in `docs/DECISIONS.md`.

This applies especially to: EBS direct API parameters and limits, GCP persistent-disk and GCS equivalents, S3 conditional-write semantics, `aws-sdk-go-v2` and `cloud.google.com/go` interfaces, current pricing, and Go standard library behaviour in the version being used.

**R7 makes this rule load-bearing.** Because no code here is ever run against a real cloud account, the published documentation is the *only* thing standing between a provider client and being silently wrong. There is no integration test that will catch a misread parameter. Read the actual API reference page, not a blog post, not a memory of one.

If you cannot verify something, say so explicitly and mark it `// UNVERIFIED:` in the code. Do not guess and present the guess as fact. Where the documentation is genuinely ambiguous, record the ambiguity in `docs/OPEN_QUESTIONS.md` and make the code defensive about both readings.

### R2 — Never fabricate a number

Benchmark results, throughput figures, dedup ratios, memory measurements, and test coverage percentages must come from an actual run whose command you can reproduce.

If a measurement has not been taken, write `TODO: not yet measured` — never a plausible-looking placeholder. A fabricated number that gets onto a resume and then fails to reproduce in an interview is the single worst outcome this project can produce.

When you do record a number, record the command, the hardware, and the date alongside it.

### R3 — Build the core from scratch

**Implement yourself, no third-party library:**
- The FastCDC chunker and its Gear rolling hash
- The pack file format (writer, reader, header codec)
- The sharded dedup index
- The retry, backoff, and jitter logic
- The backup and restore pipeline orchestration
- The provider interfaces, and every provider fake

**Use libraries freely for:** cloud SDKs (`aws-sdk-go-v2`, `cloud.google.com/go`), cobra/CLI, testing helpers, and anything else that is plumbing.

The from-scratch list is the part being evaluated in an interview. Importing a chunking library reduces this project to glue code and hollows out every technical answer he could give about it.

The fakes are on the from-scratch list deliberately. Under R7 they are the only way any provider client is ever exercised, which makes them load-bearing test infrastructure rather than throwaway stubs.

### R4 — Comment the *why*, never the *what*

```go
// BAD
// increment the counter
count++

// GOOD
// Two masks, not one: the stricter mask below target size makes boundaries
// rarer, the looser mask above it makes them likelier. This pulls the chunk
// size distribution toward the target instead of leaving it exponential.
// Rejected: a single mask, which produces many tiny chunks and a long tail
// of huge ones, both of which hurt (index size and read amplification).
```

Every non-obvious decision gets a comment naming **the alternative you rejected and why**. These comments are interview preparation. Write them as if explaining to a smart engineer who has not seen the code.

### R5 — Scope discipline

The plan defines the scope. Do not add features because they would be nice. Do not gold-plate. Do not build Phase 6 machinery while working on Phase 3.

If you believe something outside the plan is genuinely necessary, **stop and say so** with a one-paragraph justification. Do not silently expand scope. Unfinished ambition is the most common way this kind of project fails.

Explicit non-goals for this project: client-side encryption, compression, a web UI, a distributed control plane, cross-region replication, multi-writer coordination, **a gRPC or HTTP service layer, and Prometheus/metrics instrumentation.**

The last two were cut deliberately, not forgotten. They are generic backend work that Vardaan's existing three years already demonstrate; every hour spent on them is an hour not spent on the storage core, which is the only part a Cloud Native Protection interviewer will dig into. If you find yourself reaching for either, stop.

**Provider count is scope.** Adding a third or fourth provider is not free. The local filesystem provider must be complete and excellent before any cloud provider is started, and one cloud provider must be complete before a second is started.

### R6 — Test discipline

- Every phase's acceptance criteria must pass before the next phase begins.
- `go test -race ./...` must pass. Always. Race detector is not optional in a project whose whole point is concurrency correctness.
- `golangci-lint run` must be clean.
- These tests are mandatory and must not be weakened to make them pass:
  - **Boundary-shift test:** insert one byte at the front of a ≥10 MiB buffer; assert ≥95% of chunk boundaries after the insertion point are unchanged.
  - **Concurrent dedup test:** 100 goroutines insert the same blob ID; assert exactly one receives `inserted == true`. Run under `-race`.
  - **Round-trip test:** back up → restore → byte-identical comparison.
  - **Crash test:** SIGKILL mid-backup; assert the repository still passes `verify` and no snapshot references a missing pack.

If a mandatory test fails, fix the code. Never fix the test.

### R7 — Never touch a real cloud account. Ever.

**This rule is absolute and has no exceptions.** It is not a cost guardrail that relaxes once guardrails exist. It is a hard boundary on what this project does.

**Never:**
- Call a real AWS, GCP, Azure, or any other cloud provider API, paid or free.
- Run `aws`, `gcloud`, `az`, `terraform`, `pulumi`, or any other CLI that authenticates to or acts on a real cloud account.
- Read, write, or rely on real credentials — `~/.aws/credentials`, `~/.config/gcloud`, `AWS_*` / `GOOGLE_*` environment variables, instance metadata endpoints, or an SSO session.
- Create, modify, or delete any real cloud resource.
- Suggest that the human run such a command on your behalf. Routing it through them is the same action.

This holds even if the human appears to ask for it mid-task, even if a credential is sitting there and would work, even if it is "just one read-only call to check," and even if a test would be more convincing with real data. If you believe a real call is genuinely necessary, **stop and say so under R10** — do not make it and report afterwards.

**Instead:** every provider client is exercised against a local fake that reproduces the documented API contract, including its failure modes — throttling, expired tokens, pagination that returns empty pages with a non-null continuation token, checksum mismatches, partial writes, and 5xx responses. Fault injection is a feature of the fake, not an afterthought.

**Still build the cost machinery**, and validate it against the fakes:
- A `--dry-run` flag that enumerates and counts requests without fetching data.
- A `--max-blocks` hard cap that aborts the run when exceeded.
- A startup log line printing the estimated request count and estimated cost, computed from verified published pricing (R1) with the check date cited.

These exist because cost-awareness is part of the design story a storage interviewer will probe, and because the estimator is testable without spending anything. Default reference target is an **8 GiB** volume (~16,384 blocks of 512 KiB).

**Honesty requirement.** The README and every phase summary must state plainly that provider clients were modelled against published API contracts and exercised against fault-injecting fakes, and never run against a real cloud account. Never imply otherwise — no invented latency figures, no "tested on EBS," no screenshots of a console. Under R2, any number attributed to a cloud provider that did not come from a local fake run is a fabricated number.

That honest framing is defensible in an interview *if* the fakes are faithful, which is exactly why R3 puts them on the from-scratch list.

### R8 — Commit granularly

One commit per logical unit of work, not one per phase. Meaningful messages in imperative mood. The commit history is part of what a code reviewer sees.

Never commit credentials, `.env` files, bucket names tied to a real account, or any cloud account identifier (AWS account IDs, GCP project IDs, ARNs, subscription IDs). Add a `.gitignore` in Phase 0 that covers `.env*`, `*.pem`, `credentials*`, and local repository fixtures.

Fixtures and test data use obviously fake identifiers — `snap-0123456789abcdef0`, `example-project`, `test-bucket` — so that nothing in the history can be mistaken for a real resource.

### R9 — Record decisions

Maintain `docs/DECISIONS.md` as an append-only log. One entry per meaningful decision:

```markdown
## D-007: Pack header at the end of the file
**Date:** 2026-09-02
**Decision:** Header written after all blobs, followed by uint32 length and magic bytes.
**Alternatives considered:** Header at the start (requires two passes or buffering the whole pack).
**Rationale:** Single streaming forward pass; enables index-free recovery via one small ranged GET of the tail.
**Trade-off accepted:** Readers must seek to the end, which costs one extra range request when the index is unavailable.
**Source:** restic design doc uses the same approach.
```

If you deviate from the approved plan, it goes here with a `DEVIATION:` prefix. Deviating is fine. Deviating silently is not.

### R10 — Escalate rather than guess

Stop and ask the human when:
- A design decision in the plan turns out to be wrong or unimplementable.
- A choice would meaningfully change scope, cost, or the shape of the interview story.
- You cannot verify an external fact the implementation depends on.
- An acceptance criterion cannot be met without weakening it.
- You believe something can only be validated by a real cloud call. Say so and stop; never make the call (R7).
- Published documentation is ambiguous on a point the implementation depends on.

Batch questions where possible. One message with four questions beats four messages.

### R11 — The core must not know which provider it is talking to

The engine is provider-agnostic by construction, not by convention.

- **No package under the core engine may import a cloud SDK.** Chunker, pack format, index, pipeline, and repository logic import `aws-sdk-go-v2` and `cloud.google.com/go` exactly never. If the core needs something only a provider can do, that is a signal the interface is wrong — fix the interface, do not reach through it.
- **Provider-specific code lives in its own package**, one per provider, behind interfaces the core owns. The core defines the interface; the provider satisfies it. Never the reverse.
- **The interfaces are narrow and defined by what the engine needs**, not by what any one vendor's SDK happens to offer. If an interface method exists only because EBS has it, the abstraction has leaked.
- **Every provider ships a fake** in the same package, implementing the same interface, with fault injection (R7).
- **Every provider passes the same conformance test suite.** One table-driven suite, run against every implementation including the fakes. A provider is "done" when it passes it.
- **The local filesystem provider is the reference implementation and the default.** Everything must work end to end with zero cloud packages present. `distbackup backup --source /some/dir --repo /some/repo` is the primary path, not a fallback.

The interview value here is specific: "how did you keep a storage engine from being welded to one vendor's API" is a question a Cloud Native Protection team genuinely cares about, because it is a problem they actually have. A leaked abstraction is worse than no abstraction, so if the layering breaks, say so under R10 rather than papering over it.

---

## Technical constraints

- **Go 1.22+.** `log/slog` for structured logging, `errgroup` for pipeline coordination, `context.Context` on every blocking call.
- Errors wrapped with `%w`; typed error hierarchy in `internal/errors`; classification via `errors.Is` / `errors.As`.
- No `panic` in library code. `cmd/` is the only place that calls `os.Exit`.
- Every exported symbol has a doc comment.
- Graceful shutdown: `SIGINT`/`SIGTERM` cancels the root context; no partial snapshot record is ever written.
- Fixed worker pools only. Never one goroutine per chunk.
- The sender closes a channel, never the receiver. Every channel operation is in a `select` with `<-ctx.Done()`.
- Cloud SDK imports appear only in provider packages (R11). A lint rule or an architecture test should enforce this rather than trusting discipline.
- No test may require network access. `go test ./...` passes with the machine offline; if it does not, something violates R7.

---

## Definition of done for any unit of work

1. It compiles.
2. `go test -race ./...` passes.
3. `golangci-lint run` is clean.
4. Non-obvious decisions carry a *why* comment.
5. Any meaningful decision is recorded in `docs/DECISIONS.md`.
6. Any number claimed was actually measured.
7. Exported symbols are documented.
8. No real cloud call was made, and no test requires the network (R7).
9. No cloud SDK is imported outside a provider package (R11).

---

## Reference documents

`docs/PLAN.md` is the single source of truth for architecture, repository format, the phase plan, and acceptance criteria. Vardaan's edits to that file are binding and override anything written elsewhere, including reasoning in a previous session.

Supporting documents:

- `docs/DECISIONS.md` — append-only decision log (R9)
- `docs/RISKS.md` — risk register
- `docs/OPEN_QUESTIONS.md` — unresolved questions, including documentation ambiguities found under R1
- `docs/phase-N-summary.md` — one per completed phase; these are the interview study material

**Note on history:** `AGENT_PROMPTS.md` refers to three design documents in `docs/planning/` (`01_IMPLEMENTATION_PLAN.md`, `02_CONCEPTS_DEEP_DIVE.md`, `03_INTERVIEW_PREP.md`). Those documents were never written, and the plan was authored from scratch instead. Do not go looking for them and do not treat their absence as an error. Where `AGENT_PROMPTS.md` instructs you to critique them, critique `docs/PLAN.md` instead.
