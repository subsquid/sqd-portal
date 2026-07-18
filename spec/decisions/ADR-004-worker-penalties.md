# ADR-004 — Asymmetric worker penalties; one query per worker

Status: Accepted (historical, 2025-12-02)

## Context

Worker failures have very different costs to a stream. An error response returns
quickly and the chunk retries elsewhere almost for free. A timeout burns up to
P-TRANSPORT-TIMEOUT of wall-clock while the stream's front slot stalls — the single
most painful worker behavior. Meanwhile, sending several concurrent queries to one
worker concentrates risk and load on a single node when the network has thousands.

## Decision

Penalize by observed cost: a worker that errored is avoided for
P-WORKER-ERROR-COOLDOWN (short); a worker that timed out is avoided for
P-WORKER-TIMEOUT-COOLDOWN (10× longer). A worker's own backoff request is honored
(default P-WORKER-BACKOFF when unspecified). At most P-MAX-QUERIES-PER-WORKER
(currently one) query runs against a worker at a time; among healthy candidates, higher
measured throughput wins, unmeasured workers are tried first. Penalized workers remain
usable as a last resort rather than hard-excluded.

## Consequences

Streams route around slow and failing workers without permanently shrinking the pool;
load spreads across the network (good citizenship, G4). Costs accepted: the
health bookkeeping is in-memory and resets on restart (every worker starts trusted);
a worker shared across datasets shares its penalties; a slow-but-alive worker is
underused for minutes. Shapes REQ-41.
