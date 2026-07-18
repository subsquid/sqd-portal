# 01 — Overview

## What it is

The Portal is the data API layer of the SQD platform. A consumer posts a dataset query
and receives a compressed stream of block records; the Portal decides, per request,
whether the requested range is served from the **archival network** (finalized history,
held as immutable *chunks* by decentralized worker nodes) or from the **real-time
source** (a companion service retaining recent blocks near the chain head), fans the
work out, and delivers the results in strict block order.

**The hot path** the system exists for: `stream request → route by range → lease
workers / proxy to real-time source → validate, order, and forward block records →
client resumes from wherever the stream ended`. Everything else — catalog, heads,
metadata, status — exists to make that loop discoverable, resumable, and operable.

The Portal holds no durable state. Its picture of the world is rebuilt from upstream
sources on every start: a periodically fetched **assignment** (the routing artifact
mapping datasets → chunks → workers), a dataset **catalog**, and on-chain **network
status**. Worker-health bookkeeping is in-memory only and resets on restart.

### Glossary

| Term | Meaning |
|---|---|
| dataset | A named chain data collection (e.g. `ethereum-mainnet`), addressed by alias |
| block record | One line of the response stream: a block with the fields/items the query selected |
| chunk | Immutable range of finalized blocks, the archival network's unit of storage and query |
| assignment | The routing artifact published by the network: datasets, chunks, workers, and who serves what |
| worker | A network node serving archival chunk queries; identified by a peer identity |
| real-time source | Companion service holding recent (including unfinalized) blocks near the head |
| head / finalized head | Highest available block / highest finalized block of a dataset |
| data frontier | Highest block servable for a request right now (archival head or real-time head) |
| stream | One request/response pair; may end early at any time and be resumed by a follow-up request |

## Actors

| Actor | Role | Direction |
|---|---|---|
| Data consumer | Posts queries, consumes streams; typically the SQD SDK, indexers, or ad-hoc tools | inbound |
| Operator | Deploys/configures the Portal, watches metrics, locks stake for bandwidth | control |
| Archival workers | Serve chunk queries over a peer-to-peer transport | outbound |
| Assignment publisher | Publishes the assignment artifact the Portal polls | outbound |
| Dataset registry | Publishes the dataset catalog and metadata the Portal polls | outbound |
| Real-time source | Serves head/stream requests for recent blocks; proxied per request | outbound |
| Chain RPC + contracts | On-chain registry: epoch, stake, compute units, worker set | outbound |
| Error-reporting sink | Receives sampled error/trace reports | outbound |

## Design goals

| Goal | One line | Encoded by |
|---|---|---|
| G1 | Continuous, ordered, exactly-once-per-range block delivery with cheap resumption | REQ-1, REQ-2, REQ-3, REQ-6 |
| G2 | One API surface over archival history and the live head | REQ-4, REQ-5, REQ-10..REQ-13 |
| G3 | Overload is shed honestly: shed load must stay shed | REQ-20, REQ-27, ADR-007, ADR-012 |
| G4 | Be a good network citizen: self-regulate bandwidth, spread load across workers, account for usage | REQ-40..REQ-44, ADR-006, ADR-004 |
| G5 | Operable: truthful readiness, observable behavior, forgiving configuration | REQ-23, REQ-24, REQ-30..REQ-33 |
| G6 | Robust against hostile clients and flaky upstreams | REQ-21, REQ-22, REQ-25, REQ-26 |

## Non-goals

| Non-goal | Rationale |
|---|---|
| NG1 — No per-request authentication or authorization | The Portal runs behind a trusted perimeter; any per-client policy (identity, tiers, head lag) originates upstream of it (ADR-009). A Portal reachable directly by untrusted clients has no client-level defenses beyond input validation. |
| NG2 — No per-client quotas or fairness | All capacity limits are global. One client can exhaust shared capacity; isolation between clients is not promised. |
| NG3 — No head subscription or long-poll | Clients poll. A request beyond the frontier gets a throttled empty response (REQ-5), never a held-open wait for new blocks. |
| NG4 — No cross-source splicing within one response | Each response is served entirely by one source (archival or real-time). Crossing the boundary is the client's follow-up request (REQ-4). |
| NG5 — No durable local state | Restart amnesia is by design: everything is refetched or relearned. There is nothing to back up or recover. |
| NG6 — SQL surface plans, never executes | The experimental SQL endpoints return a routing plan (which workers hold which chunks); execution happens elsewhere (REQ-15). |
| NG7 — Deprecated endpoints are unspecified | Legacy routes (worker lookup, height, direct worker query) exist for migration only; their behavior must not be pinned by new clients or tests. |

## Trust model

| Actor | Verified | Trusted | Must never be able to cause |
|---|---|---|---|
| Data consumer | Query syntax, size caps, parameter ranges | Nothing | Crash/wedge the process, corrupt another stream, bypass operator caps (REQ-21) |
| Archival worker | Response signature (when enabled, REQ-43); response size cap; result range plausibility | Data content within its signed response | Process crash, unbounded memory, permanently poisoning the worker pool (penalties decay, REQ-41) |
| Assignment publisher | Transfer integrity only — structural validity is currently **trusted, not verified** (ADR-002, GAP-1) | Artifact correctness | *Intent:* crash or false readiness via a corrupt artifact (REQ-26) — presently not enforced |
| Real-time source | Streamed success data and public headers; errors normalized by ADR-011 | Data content, conflict responses | Stalling a client past bounded deadlines (REQ-22) |
| Chain RPC | Nothing beyond transport | Status values | Any effect on data serving — status only (REQ-25) |
| Operator | Config validity at startup | Fully | — |

## Lifecycle at a glance

```
        assignment publisher      dataset registry        chain RPC / contracts
              │ poll P-ASSIGNMENT-REFRESH │ poll P-DATASETS-REFRESH │ poll P-CHAIN-REFRESH
              ▼                           ▼                         ▼
        ┌───────────────────────── PORTAL (in-memory world view) ─────────────────────────┐
        │  routing: dataset → chunks → workers        catalog, heads, network status      │
        └──────────────────────────────────────────────────────────────────────────────────┘
 consumer ──POST stream──▶  route by range ──┬── fan out chunk queries ──▶ archival workers
          ◀── ordered block records ─────────┤        (lease, retry, order, verify)
                                             └── proxy ──▶ real-time source
```

- **Request lifecycle:** validate → clamp to operator bounds → route by range → serve
  from one source → end (complete, empty, or truncated) → client resumes (REQ-2).
- **Process lifecycle:** start → fetch catalog + first assignment → ready → serve ⟲
  refresh world view → SIGTERM → advertise not-ready, drain two-phase (REQ-24) → exit.
