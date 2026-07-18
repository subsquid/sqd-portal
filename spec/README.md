# SQD Portal — spec suite

SQD Portal is an HTTP service that exposes blockchain data as continuous streams. It sits
between data consumers and two data planes: the SQD Network (a decentralized archive of
finalized chain data served by worker nodes) and a real-time block source near the chain
head. It fans queries out, validates and orders the results, and delivers them as one
resumable stream per request.

## Purpose, tier, shape

This suite records **what the Portal must do** (numbered requirements with acceptance
criteria), **why it is that way** (a decision log), **the precise behavioral contract**
(definitions, operation and dependency contracts, invariants, liveness, failure model),
and **how conformance is established** (a reference-model oracle, test taxonomy,
traceability matrix, SLOs, and a dated gap register). It is written at the
**conformance tier** for the **stateless service** shape (a fan-out proxy/streamer;
nothing durable survives a restart). Modules 06 (consistency/durability) and 10
(domain lifecycle) are absent by shape: the Portal owns no persistent state, and its
refreshed-snapshot lifecycle is folded into 05. Numbering gaps are canonical, never
recycled.

## Document map

| Doc | Contents | Normative? | Mutable? |
|---|---|---|---|
| [01-overview.md](01-overview.md) | context, actors, goals, non-goals, trust model, lifecycle | yes | no |
| [02-requirements.md](02-requirements.md) | requirements `REQ-n` with acceptance criteria; unspecified list; open questions `OQ-n` | yes | no |
| [03-data-model.md](03-data-model.md) | definitions `DEF-n`: objects, shared state, error taxonomy, terminology map | yes | no |
| [04-operations.md](04-operations.md) | operation contracts `OP-n`; the response-purity rule; concurrency | yes | no |
| [05-dependencies.md](05-dependencies.md) | dependency contracts `DC-n`; error mapping; caches & staleness | yes | no |
| [07-invariants.md](07-invariants.md) | safety catalog `INV-n`, scope-tagged, with check strategies | yes | no |
| [08-liveness.md](08-liveness.md) | liveness `LIV-n`: environment defs, bounds, witnesses | yes | no |
| [09-failure-model.md](09-failure-model.md) | fault families `FM-n` × required responses (mask/degrade/fail-safe/alarm) | yes | no |
| [11-performance.md](11-performance.md) | workload model, `SLI-n`/SLO table, resource bounds `PF-n`, hazards `HZ-n` | yes (targets ⚠) | no |
| [12-observability.md](12-observability.md) | required signals `OB-n`; property → observable mapping | yes | no |
| [13-conformance.md](13-conformance.md) | harness, reference model & free variables, `CT-n` taxonomy, traceability matrices, gap register `GAP-n`, build order | no (status) | **yes** |
| [14-interface-binding.md](14-interface-binding.md) | the concrete HTTP surface `IB-n`: routes, codes, headers, schemas | yes | no |
| [15-parameters.md](15-parameters.md) | every `P-*` symbol: role, observed value, target | no (registry) | **yes** |
| `decisions/` | one ADR per decision, append-only | rationale | append-only |

ADR numbers follow decision date. Historical decisions come first; decisions that have
not been ratified are explicitly `Proposed` and appear after the accepted log.

| ADR | Date | Title | Status |
|---|---|---|---|
| ADR-001 | 2024-08-28 | [Mid-stream failure truncates the response; continuation is the recovery path](decisions/ADR-001-truncation-over-error.md) | Accepted (historical) |
| ADR-002 | 2025-08-08 | [Assignment is loaded without structural verification](decisions/ADR-002-skip-assignment-verification.md) | Accepted (historical) |
| ADR-003 | 2025-09-15 | [Real-time data is served through a streaming Portal proxy](decisions/ADR-003-real-time-proxy.md) | Accepted (historical; amended by ADR-011) |
| ADR-004 | 2025-12-02 | [Asymmetric worker penalties; one query per worker](decisions/ADR-004-worker-penalties.md) | Accepted (historical) |
| ADR-005 | 2026-05-27 | [Two-phase graceful shutdown](decisions/ADR-005-two-phase-shutdown.md) | Accepted (historical) |
| ADR-006 | 2026-06-03 | [Network bandwidth is the modeled limiting resource](decisions/ADR-006-bandwidth-limiting-resource.md) | Accepted (historical) |
| ADR-007 | 2026-06-04 | [Overload gets its own status code, distinct from data unavailability](decisions/ADR-007-overload-529.md) | Accepted (historical) |
| ADR-008 | 2026-06-08 | [Unknown config fields warn, don't reject](decisions/ADR-008-config-unknown-fields-warn.md) | Accepted (historical) |
| ADR-009 | 2026-06-25 | Per-request artificial head latency for real-time streams *(canonical record kept in the sqd-network decision log; not bundled in this suite — see OQ-5)* | Accepted |
| ADR-010 | 2026-07-09 | [Upstream deadlines sit strictly below client deadlines](decisions/ADR-010-upstream-deadline-below-client.md) | Accepted (historical) |
| ADR-011 | 2026-07-16 | [Portal API errors use a stable two-axis taxonomy](decisions/ADR-011-error-taxonomy.md) | Accepted |
| ADR-012 | 2026-07-17 | [Every shed response carries an explicit retry hint](decisions/ADR-012-retry-after-on-shed.md) | Accepted |
| ADR-014 | 2026-07-17 | [Contract reconciliation: overload hints, conflict precedence, artifact regression, archival finalized head, EMPTY metadata, timestamp frontier, gated debug surface](decisions/ADR-014-contract-reconciliation.md) | Accepted |
| ADR-013 | 2026-07-17 | [Assignment staleness must be bounded and observable](decisions/ADR-013-assignment-staleness-bound.md) | **Proposed** |

## Conventions

- RFC 2119 keywords (MUST / SHOULD / MAY); every requirement carries one tag.
- IDs are stable and **banded** — numbering leaves gaps between capability areas so
  additions never renumber. Prefixes: `REQ` (requirement), `DEF` (definition), `OP`
  (operation), `DC` (dependency contract), `INV` (invariant), `LIV` (liveness), `FM`
  (failure-model rule), `PF` (resource bound), `SLI` (indicator), `HZ` (hazard), `OB`
  (observable), `CT` (test class), `IB` (binding rule), `FV` (oracle free variable),
  `GAP` (registered deviation), `OQ` (open question), `ADR-nnn` (decision), `G`/`NG`
  (goals / non-goals, in 01).
- Scope tags on invariants: `[state]` / `[transition]` / `[response]` / `[recovery]`.
- Every constant appears in normative text only as a symbolic parameter `P-NAME`;
  concrete values live exclusively in [15-parameters.md](15-parameters.md). ⚠ marks a
  proposed value awaiting ratification via ADR.
- **Two mutable docs, one append-only log.** Statuses, dates, and current values change
  only in 13 and 15. `decisions/` only ever gains files; an accepted ADR is never edited,
  only superseded. All other docs change only when *intended behavior* changes.
- Normative text is implementation-free: no library, framework, or internal module
  names. Observable wire behavior (HTTP status codes, header names, encodings) is
  contract, not implementation, and may appear in acceptance criteria. ADRs are exempt
  and may name concrete technology.

## How to use this spec

1. **Extend the harness past Phase 0** (13 §build order): the Phase-0 skeleton —
   dependency stubs per IB-7, the structural validators, the reference model — has
   landed (GAP-14 closed 2026-07-17); build the `CT-2..CT-9` classes on it next.
2. **Ratify or reject proposed ADR-013** and the ⚠ targets in [15-parameters.md](15-parameters.md)
   and the SLO table (11); close the open questions in
   [02-requirements.md](02-requirements.md).
3. **Burn down the gap register** in [13-conformance.md](13-conformance.md) in priority
   order — failing test first, fix second, matrices updated in the same change.
