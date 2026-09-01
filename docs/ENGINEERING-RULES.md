# Engineering rules

The standards this repository is built to. They are referenced by number from
code comments and from the other documents in `docs/`, and they are the reason
several things here look more careful than a project this size usually is.

---

## R1 — Verify external facts; never assume them

Before writing code that depends on an external fact — an API parameter, a
limit, a price, a library signature — verify it against the current official
documentation, and cite the source with the date it was checked, in a comment
or in `DECISIONS.md`.

**R7 makes this load-bearing.** Because no code here is ever run against a real
cloud account, published documentation is the *only* thing standing between a
provider client and being silently wrong. There is no integration test that
will catch a misread parameter. Read the actual API reference page — not a
blog post, not a memory of one.

Where documentation is genuinely ambiguous, record the ambiguity in
`OPEN_QUESTIONS.md` and make the code defensive about both readings.

## R2 — Never fabricate a number

Benchmark results, throughput figures, deduplication ratios, memory
measurements, and coverage percentages must come from an actual run whose
command can be reproduced. If a measurement has not been taken, say
`TODO: not yet measured` — never a plausible-looking placeholder.

When a number is recorded, record the command, the hardware, and the date
alongside it.

*This rule has already earned its keep once: see the benchmark correction noted
in the README's index-sharding section.*

## R3 — Build the core from scratch

Implemented here, with no third-party library:

- the FastCDC chunker and its Gear rolling hash
- the pack file format (writer, reader, header codec)
- the sharded deduplication index
- the retry, backoff and jitter logic
- the backup and restore pipeline orchestration
- the provider interfaces, and every provider fake

Libraries are used freely for cloud SDKs and other plumbing.

The fakes are on this list deliberately. Under R7 they are the only way any
provider client is ever exercised, which makes them load-bearing test
infrastructure rather than throwaway stubs.

## R4 — Comment the *why*, never the *what*

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

Every non-obvious decision carries a comment naming **the alternative that was
rejected and why**.

## R5 — Scope discipline

The plan defines the scope. No features because they would be nice, no
gold-plating, no building later phases while an earlier one is unfinished.

Explicit non-goals: client-side encryption, compression, a web UI, a
distributed control plane, cross-region replication, multi-writer
coordination, a service layer, and metrics instrumentation.

**Provider count is scope.** The local filesystem provider must be complete
before any cloud provider is started, and one cloud provider must be complete
before a second is.

## R6 — Test discipline

- `go test -race ./...` must pass. Always. The race detector is not optional
  in a project whose whole point is concurrency correctness.
- `golangci-lint run` must be clean.
- Four tests are mandatory and must never be weakened to make them pass:
  - **Boundary shift:** insert one byte at the front of a ≥10 MiB buffer;
    assert ≥95% of chunk boundaries after the insertion point are unchanged.
  - **Concurrent dedup:** 100 goroutines insert the same blob ID; assert
    exactly one receives `inserted == true`. Run under `-race`.
  - **Round trip:** back up → restore → byte-identical comparison.
  - **Crash:** SIGKILL mid-backup; assert the repository still passes `verify`
    and that no snapshot references a missing pack.

If a mandatory test fails, fix the code. Never fix the test.

A test that cannot fail proves nothing, so each of these was additionally
verified to fail against a deliberately broken implementation.

## R7 — Never touch a real cloud account

**Absolute, and without exceptions.**

Never call a real AWS, GCP, or Azure API, paid or free. Never run a CLI that
authenticates to a real cloud account. Never read or rely on real credentials,
environment variables, or instance metadata. Never create, modify, or delete a
real cloud resource.

**Instead:** every provider client is exercised against a local fake that
reproduces the documented API contract *including its failure modes* —
throttling, expired tokens, pagination that returns empty pages with a
non-null continuation token, checksum mismatches, and partial writes. Fault
injection is a feature of the fake, not an afterthought.

The cost machinery is still built and validated against the fakes: a
`--dry-run` flag that enumerates work without fetching data, a hard cap that
aborts an over-budget run, and a startup line reporting estimated request
count and cost computed from verified published pricing.

**Honesty requirement.** The README and every summary must state plainly that
provider clients were modelled against published API contracts and exercised
against fault-injecting fakes, and never run against a real cloud account.
Under R2, any number attributed to a cloud provider that did not come from a
local fake run is a fabricated number.

That framing is defensible *if* the fakes are faithful, which is exactly why
R3 puts them on the from-scratch list.

## R8 — Commit granularly

One commit per logical unit of work, not one per phase. Meaningful messages in
the imperative mood.

Never commit credentials, `.env` files, or any cloud account identifier. Test
fixtures use obviously fake identifiers — `snap-0123456789abcdef0`,
`test-bucket` — so nothing in the history can be mistaken for a real resource.

## R9 — Record decisions

`DECISIONS.md` is an append-only log. One entry per meaningful decision, each
naming the alternatives considered, the rationale, the trade-off accepted, and
the source. Deviating from the plan is fine; deviating silently is not.

## R10 — Surface ambiguity rather than guessing

Stop and raise it when a design decision turns out to be wrong or
unimplementable, when a choice would meaningfully change scope, when an
external fact cannot be verified, when an acceptance criterion cannot be met
without weakening it, or when published documentation is ambiguous on a point
the implementation depends on.

## R11 — The core must not know which provider it is talking to

The engine is provider-agnostic by construction, not by convention.

- **No core package may import a cloud SDK.** Chunker, pack format, index,
  pipeline, and repository logic import cloud SDKs exactly never. If the core
  needs something only a provider can do, the interface is wrong — fix the
  interface, do not reach through it.
- **Provider code lives in its own package**, behind interfaces the core owns.
  The core defines the interface; the provider satisfies it. Never the reverse.
- **Interfaces are narrow and defined by what the engine needs**, not by what
  a vendor's SDK happens to offer. If a method exists only because EBS has it,
  the abstraction has leaked.
- **Every provider ships a fake** in the same package, with fault injection.
- **Every provider passes the same conformance test suite.** A provider is
  "done" when it passes it.
- **The local filesystem provider is the reference implementation and the
  default.** Everything works end to end with zero cloud packages present.

This is enforced by an architecture test that parses the import graph and
fails the build on a violation — not by code-review discipline.

---

## Technical constraints

- Go 1.24+. `log/slog` for structured logging, `context.Context` on every
  blocking call.
- Errors wrapped with `%w`; typed error hierarchy in `internal/errs`;
  classification via `errors.Is` / `errors.As`.
- No `panic` in library code. `cmd/` is the only place that calls `os.Exit`.
- Every exported symbol has a doc comment.
- Graceful shutdown: `SIGINT`/`SIGTERM` cancels the root context; no partial
  snapshot record is ever written.
- Fixed worker pools only. Never one goroutine per chunk.
- The sender closes a channel, never the receiver. Every channel operation is
  in a `select` with `<-ctx.Done()`.
- No test may require network access. `go test ./...` passes offline; if it
  does not, something violates R7.

## Definition of done

1. It compiles.
2. `go test -race ./...` passes.
3. `golangci-lint run` is clean.
4. Non-obvious decisions carry a *why* comment.
5. Any meaningful decision is recorded in `DECISIONS.md`.
6. Any number claimed was actually measured.
7. Exported symbols are documented.
8. No real cloud call was made, and no test requires the network (R7).
9. No cloud SDK is imported outside a provider package (R11).
