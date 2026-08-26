# Open questions

Things that could not be resolved, and what would resolve them.

---

## Q-001: Does re-listing a snapshot invalidate outstanding EBS block tokens?

**Status:** unresolved — documentation is ambiguous.

The EBS direct API concepts page states block tokens "change on the expiry timestamp specified for them, **or if you run another `ListSnapshotBlocks` or `ListChangedBlocks` request for the same snapshot**." The FAQ states block tokens are valid for **seven days**.

These are not obviously consistent. Two readings:

1. **Strong:** a new listing invalidates every previously issued token for that snapshot. A long backup that re-lists to refresh pagination would destroy the tokens it is currently working through.
2. **Weak:** a new listing merely issues *different* token values for the same blocks; previously issued tokens remain valid for their 7 days.

**Why it matters:** it decides whether the reader may re-list concurrently with fetching. Under the strong reading, a single listing pass must complete and its tokens must be consumed before any re-list — which constrains resumability, because pagination tokens expire after 60 minutes while block tokens last 7 days.

**What would resolve it:** an authoritative statement in the API reference, or an empirical test against a real snapshot. The empirical route is closed by R7 (never touch a real cloud account), so this stays open.

**How the code handles it:** defensively, for the strong reading. The EBS source performs one listing pass, materialises the block references it needs, and never re-lists a snapshot while tokens from a prior listing are in flight. The fake implements the strong reading so tests exercise the stricter contract. Marked `// UNVERIFIED:` at the call site.

---

## Q-002: Real-world throughput and latency characteristics of the EBS direct API

**Status:** unresolvable within this project's constraints.

Per-request latency, achievable parallelism before throttling, and realistic throughput per snapshot are not published as hard numbers, and R7 forbids measuring them.

**Consequence:** the concurrency defaults for cloud sources (`min(32, 4×NumCPU)` readers) are reasoned, not measured. The README must not present them as tuned. Any throughput figure in this project comes from local providers only, and must say so.

**What would resolve it:** a real run, which is out of scope by decision D-002.

---

## Q-003: Index size per TiB has not been measured

**Status:** open until Phase 13 benchmarks run.

D-004 argues ~1 GiB of index per TiB of unique data from ~16M chunks × ~60 bytes/entry. Both factors are estimates: the true average chunk size depends on the measured distribution, and the true per-entry cost depends on the final struct layout and map overhead.

**What would resolve it:** the Phase 13 benchmark, which measures actual bytes retained for a known corpus. Until then, the figure is marked `TODO: not yet measured` everywhere it appears outside this file.
