# ADR-007 — Overload gets its own status code (529), distinct from data unavailability

Status: Accepted (historical, 2026-06-04)

## Context

The Portal returned 503 both when it was saturated (a condition the operator can fix by
locking more compute units or adding replicas) and when the network genuinely had no
worker for the requested data. Dashboards and clients could not tell "this portal is
full" from "the network can't serve this", and 503 also reads as a server fault rather
than a capacity signal. The streaming layer had just been reorganized around bandwidth
as the limiting resource (ADR-006), which made portal-local saturation a first-class,
recognizable state.

## Decision

Portal-local overload (stream cap, bandwidth saturation, all workers rate-limiting us)
returns HTTP **529** ("site is overloaded", non-standard). 503 is reserved for genuine
data unavailability — no worker currently holds the chunk.

## Consequences

Clients and dashboards can react differently to "back off, this portal is busy" versus
"this data cannot be served right now". The cost is a non-standard status code that
intermediaries may not recognize (and could in principle rewrite). A leftover
inconsistency: one deprecated endpoint still signals backoff with 429 — deprecated
routes are explicitly unspecified (NG7). Shapes REQ-20; refined by ADR-012.

**Implementation status:** not yet integrated on current master — the stream-cap path
still returns bare 503 (`RequestError::Unavailable`), not 529. Tracked as GAP-4 (P1).
