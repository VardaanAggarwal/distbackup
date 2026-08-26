# Agent Prompts — distbackup

Four prompts, used in order. Copy the block, paste it into Claude Code, nothing else in that message.

## Setup before you start

```bash
mkdir -p distbackup/docs/planning && cd distbackup
git init
# copy 01_IMPLEMENTATION_PLAN.md, 02_CONCEPTS_DEEP_DIVE.md, 03_INTERVIEW_PREP.md
#   into docs/planning/
# copy CLAUDE.md into the repo root
```

Then open Claude Code in that directory and send **Prompt 1**.

**Sequence:** Prompt 1 (research + plan) → you review and edit `docs/PLAN.md` → Prompt 2 (build) → Prompt 3 after each phase → Prompt 4 when you're preparing for interviews.

---

# PROMPT 1 — Research and planning

> Send this first. It should produce a plan, not code. If the agent starts writing Go, stop it and re-send.

```
You are working on `distbackup`, a content-addressed deduplicating backup engine
for AWS EBS and S3, written in Go. Read CLAUDE.md at the repo root first — those
rules govern everything you do here, in this session and every future one.

## This session is PLANNING ONLY

You will produce a plan. You will not write implementation code, create Go source
files, or run `go build`. The only files you may create are the deliverables listed
at the bottom of this message. If you find yourself writing Go, you have gone off
track — stop and return to planning.

There is a hard approval gate at the end of this session. Do not cross it on your own.

## Your inputs

Read these three documents in `docs/planning/`:

  01_IMPLEMENTATION_PLAN.md   architecture, repository format, phase plan
  02_CONCEPTS_DEEP_DIVE.md    the reasoning behind each technique
  03_INTERVIEW_PREP.md        what the human must be able to defend

Treat them as a serious first draft by someone who had not yet seen the code.
Your job is not to restate them. Your job is to pressure-test them, correct them
where they are wrong, fill what they left out, and turn them into an executable plan.

## What I want you to do, in order

### Step 1 — Verify every external fact

The source documents were written against a knowledge cutoff. Independently verify
against current official documentation, and flag anything that has changed:

  - EBS direct APIs: exact parameters, limits, block size, token expiry semantics,
    the `ChangedBlock.FirstBlockToken` absent-means-new behaviour, pagination bounds,
    and whether any newer API supersedes them
  - Current EBS direct API and S3 pricing in ap-south-1 and us-east-1
  - `aws-sdk-go-v2` current interfaces for `ebs` and `s3`, the transfer manager,
    and retry mode configuration — confirm the exact call signatures
  - S3 conditional write semantics (`If-None-Match`, `If-Match`), which operations
    support them, and what error code a failed precondition returns
  - Current stable Go version and anything in the standard library that has moved
  - grpc-go and protobuf tooling: current recommended codegen setup

For each fact: state what the source document claimed, what you found, and whether
they agree. Where they disagree, the verified fact wins.

### Step 2 — Challenge the design

Go through the design decisions in section 4 of 01_IMPLEMENTATION_PLAN.md and the
repository format in section 5. For each, tell me honestly:

  - Is the reasoning sound?
  - Is there a materially better alternative? If so, what does it cost?
  - What did the document get wrong or leave dangerously underspecified?

I specifically want your independent judgement on:

  a. FastCDC parameters — is min 16 KiB / target 64 KiB / max 256 KiB / normalization 2
     right for this project's data sizes and index memory budget? Show the reasoning.
  b. The fixed-512-KiB-for-EBS versus CDC-for-files split. Is it correct? What breaks?
  c. The repository format — is anything missing that would make the format
     un-evolvable, un-recoverable, or unsafe under concurrent access?
  d. The write-ordering crash-safety argument. Find a crash window it does not cover.
  e. Whether the 3-week / ~60-hour estimate is realistic for someone working
     3-4 hours on weekdays alongside other commitments. If it is not, say so plainly
     and tell me what to cut. I would rather ship less and finish.

Disagree with me where you think I am wrong. A plan I have not stress-tested is
worth less than one you argued with. Do not be agreeable for its own sake.

### Step 3 — Find what is missing

What does a real implementation need that neither document covers? Look for gaps in:
error taxonomy, configuration handling, resumability of an interrupted backup,
partial-pack recovery, index rebuild, format versioning and forward compatibility,
clock skew, and the local development story that avoids AWS entirely.

### Step 4 — Ask me your questions

Batch them into one list. Maximum seven. Only things that actually change the plan —
do not ask me to confirm things the documents already answer.

Do not proceed past this step until I answer.

## Deliverables

After I answer your questions, produce exactly these files:

**`docs/PLAN.md`** — the executable build plan and the single source of truth from
here on. It must contain:
  - Verified technical facts, with sources, in a form the build phase can rely on
  - Final architecture and package layout (yours, not a copy of mine)
  - Complete repository format spec, versioned, with a forward-compatibility note
  - The concurrency model, with channel capacities and worker counts and the
    reasoning for each
  - A phase-by-phase build plan. For each phase: goal, files touched, the tests
    that must pass, machine-checkable acceptance criteria, and an hour estimate
  - Cost guardrails and the exact sequence of the first real AWS run
  - The full list of benchmarks to run, with the exact commands

**`docs/RISKS.md`** — a risk register. Each risk: what could go wrong, likelihood,
blast radius, early warning sign, and mitigation. Include the ones that cost time
(concurrency bugs, buffer-reuse aliasing, EBS token expiry) and the ones that cost
money (runaway API calls).

**`docs/DECISIONS.md`** — seeded with every decision you have already made or
changed, in the format specified in CLAUDE.md.

**`docs/OPEN_QUESTIONS.md`** — anything you could not resolve, and what would resolve it.

## Then stop

End your final message with a summary of: what you changed from my draft and why,
what you are least confident about, and what you recommend cutting if time runs short.

Then stop and wait. Do not write any implementation code until I reply with the
exact string:

    APPROVED — BEGIN PHASE 0

I will read your plan, edit `docs/PLAN.md` directly, and send that string when ready.
Treat my edits to that file as binding.
```

---

# PROMPT 2 — Begin the build

> Send only after you have read and edited `docs/PLAN.md`. The approval string must appear exactly.

```
APPROVED — BEGIN PHASE 0

I have reviewed and edited `docs/PLAN.md`. Re-read it now — my edits are binding and
override anything you wrote earlier or anything in `docs/planning/`. Where my edits
conflict with your original reasoning, follow my edits; if you think an edit is a
mistake, say so before you implement it rather than after.

## How you work from here

**One phase at a time.** Build only the phase you are on. Do not scaffold future
phases, do not add abstractions "for later," do not implement a feature because you
are already in the file. Scope creep is the failure mode I am most worried about.

**Do not start the next phase on your own.** When a phase is complete, stop and
report. I will tell you when to continue.

**Test discipline.** Write the tests named in the phase's acceptance criteria
alongside the implementation, not after. The four mandatory tests in CLAUDE.md R6
must never be weakened to pass — if one fails, the code is wrong.

**Verify before you claim.** Every phase report must contain the actual output of
`go test -race ./...` and `golangci-lint run`. Not a summary of it. The output.

**Teach as you go.** Remember that I will be interviewed on this code by senior
storage engineers. When you implement something non-obvious, the comment explains
the alternative you rejected and why. When you finish a phase, the summary explains
it to me as an engineer, not as a changelog.

## After each phase, produce `docs/phase-N-summary.md`

  1. **What was built** — the components, in plain language
  2. **Key decisions** — each with the alternative rejected and why (also append to DECISIONS.md)
  3. **How it works** — walk me through the main code path as if teaching it
  4. **Test output** — the real, pasted output of the race tests and the linter
  5. **Measurements** — real numbers with the exact command and hardware, or
     `TODO: not yet measured`. Never a plausible-looking guess.
  6. **What a reviewer would challenge** — the three hardest questions a senior
     engineer could ask about this phase, and how you would answer them
  7. **Deviations** — anything you did differently from PLAN.md, with reasoning
  8. **What is not done** — known gaps, deferred work, rough edges

Then stop and wait for me.

## Standing reminders

  - Implement FastCDC, the pack format, the sharded index, and the retry logic
    from scratch. AWS SDK, cobra, grpc-go, and Prometheus client are fine.
  - No paid AWS API call until `--dry-run`, `--max-blocks`, and the cost estimate
    log line all exist and work.
  - Commit granularly with meaningful messages, never one commit per phase.
  - If something in PLAN.md turns out to be wrong once you are in the code, stop
    and tell me. Do not silently work around it.

Begin Phase 0.
```

---

# PROMPT 3 — Continue to the next phase

> Send after reviewing each phase summary. Short by design.

```
Phase N summary reviewed. [Any corrections or changes I want.]

Before you continue, answer these:
  - What in the last phase are you least confident about?
  - Is there anything you built that I would struggle to explain in an interview?
    If so, simplify it now rather than later.

Then proceed to Phase N+1 under the same rules: one phase only, tests alongside the
implementation, real test output in the report, then stop.
```

---

# PROMPT 4 — Interview readiness review

> Send when the project is done and you are preparing for interviews. This is the one that converts code into recall.

```
The project is built. Switch roles: you are now a senior storage engineer at Rubrik
preparing to interview me on this codebase.

## Part 1 — Audit

Read the whole repository. Then tell me honestly:

  - Where does the implementation not match what `docs/PLAN.md` and the README claim?
    Every gap, including small ones. I need to know before an interviewer finds it.
  - Which numbers in the README or benchmarks would fail to reproduce if someone
    asked me to run them live right now?
  - What is the single weakest part of this codebase, and what is the honest answer
    if I am asked about it?
  - What would a senior engineer find sloppy on a fifteen-minute skim?

## Part 2 — Explain the parts I would struggle with

Identify the five files or functions I would have the hardest time explaining cold.
For each, write an explanation I can actually learn from: what it does, why it works
this way, what the alternative was, and what breaks if you change it.

## Part 3 — Interview me

Ask me questions one at a time, hardest first, and wait for my answer before moving on.
Cover: chunking and dedup, storage format and crash safety, the concurrency model,
AWS specifics and failure handling, and scaling limits.

After each answer, tell me plainly whether it would satisfy a senior interviewer,
what was missing, and what the stronger version of the answer sounds like. Do not be
generous — an inflated assessment now costs me the offer later.

Then update `docs/planning/03_INTERVIEW_PREP.md` with anything I answered badly and
anything the project does that the prep document does not cover.
```

---

## Notes on using these

**Watch for the two failure modes.** The agent will drift toward writing code during Prompt 1, and toward building ahead during Prompt 2. Both are worth interrupting immediately — a phase built out of order is harder to unwind than to redo.

**Your edits to `docs/PLAN.md` are the real leverage.** The agent's plan will be better than the draft I wrote because it can verify current facts, but it will still be biased toward building more. Cutting scope in that file is the highest-value thing you do in this whole process.

**Do not skip the phase summaries.** They are the difference between a project you shipped and a project you can defend. If you find yourself skimming them, that is the signal you are heading toward the "how much of this did an AI write" question with no good answer.

**When the agent disagrees with the plan, take it seriously.** It can read the current AWS docs; I was working from a May 2026 cutoff. If it says the EBS API or the SDK signature has changed, it is probably right.
