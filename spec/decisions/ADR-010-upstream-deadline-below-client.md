# ADR-010 — Upstream deadlines sit strictly below client deadlines

Status: Accepted (historical, 2026-07-09)

## Context

The Portal's read timeout on the real-time upstream was 30 s — exactly the dominant
client SDK's request deadline. When the upstream stalled, the caller's deadline fired
at the same instant the Portal would have answered: the client saw an opaque timeout,
and the Portal recorded **nothing** (the request never completed, so no status metric
fired). Stalls were invisible except in caller logs. Separately, several outbound calls
had no deadline at all.

## Decision

Every outbound call carries a deadline, and deadlines on the client-facing request path
are ordered: the upstream read deadline (P-HOTBLOCKS-READ-TIMEOUT, default 20 s) sits
strictly below the client SDK deadline (P-CLIENT-SDK-TIMEOUT, 30 s) and above the
upstream's own internal budgets (its 5 s long-poll + 10 s query), so a stall surfaces
as the Portal's own 502 — attributed, logged, and counted — before the client gives up.

## Consequences

Stalled upstreams become a Portal-observable failure class with metrics and logs,
instead of a silent client-side mystery. The deadline ordering is a standing constraint
on configuration: raising the upstream deadline above the client deadline silently
reintroduces the blindness. Shapes REQ-22.
