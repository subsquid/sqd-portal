# ADR-006 — Network bandwidth is the modeled limiting resource

Status: Accepted (historical, 2026-06-03; rationale partly reconstructed)

## Context

Under heavy data-streaming load the scarce resource is download bandwidth from workers,
not CPU or connection count. Overload manifested as saturated chunk downloads slowing
every stream at once. A fixed concurrency limit either wastes capacity on light
workloads or thrashes on heavy ones. (The decision is evidenced by the streaming-layer
reorganization "around the network bandwidth being the limiting resource"; the specific
choice of an AIMD scheme over alternatives is reconstructed, not documented —
inference.)

## Decision

Govern concurrent chunk-body downloads with a single global adaptive window: additive
increase on success, multiplicative decrease (P-CONGESTION-DECREASE) on congestion
signals (read stalls past P-CONGESTION-READ-TIMEOUT, transport errors), bounded to
[P-CONGESTION-MIN-WINDOW, P-CONGESTION-MAX-WINDOW], shrinking at most once per
P-CONGESTION-SHRINK-INTERVAL. Download slots are granted by priority:
oldest-stream-first, earliest-chunk-first (stride P-PRIORITY-STRIDE). When window
utilization exceeds P-HEADROOM-THRESHOLD, new streams are refused as overload
(ADR-007/ADR-012) rather than admitted to degrade everyone.

## Consequences

Throughput adapts to actual network conditions and degrades gracefully instead of
collapsing. The window is global: congestion caused by any worker or dataset throttles
all — an accepted coupling (NG2, no per-client fairness). Only body downloads are
metered; first-byte waits are not (part of the GAP-3 memory concern). Shapes REQ-42,
REQ-20.
