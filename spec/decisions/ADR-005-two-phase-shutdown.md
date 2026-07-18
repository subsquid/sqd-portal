# ADR-005 — Two-phase graceful shutdown

Status: Accepted (historical, 2026-05-27)

## Context

Under rolling deploys, load balancers need time to observe a not-ready state before the
listener actually stops, or they keep routing to a dying instance. But the signal that
flips readiness must not also cancel background tasks — cancelling the network client
early would break in-flight streams during the drain. Alternatives considered and
rejected: one primitive for both concerns (either cancels too early or keeps readiness
lying); per-spawned-task cancellation tokens (doesn't scale, orphans die with the
runtime anyway); handling SIGINT the same way (developer Ctrl-C must stay instant);
Windows support (deployment is Linux-only).

## Decision

Two independent primitives, two phases. On SIGTERM: (1) flip a shutdown flag —
readiness immediately reports not-ready — while serving continues for
P-PRE-DRAIN-GRACE; (2) then fire a cancellation signal: stop intake, drain in-flight
connections for at most P-DRAIN-TIMEOUT, then detach whatever remains and exit.

## Consequences

Shutdown is bounded by P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT (50 s at current values);
the orchestrator's kill grace must exceed it — recommend ≥ 60 s (Kubernetes
`terminationGracePeriodSeconds`) to leave headroom for network-client wind-down, Sentry
flush, and runtime drop. Clients on streams longer than the drain
window see truncation, recovered per REQ-6/REQ-2. Cooperative per-stream wind-down and
a network-client wind-down timeout were explicit non-goals. If frequent hard kills
appear, the sanctioned escalation is an explicit early process exit after force-close.
Shapes REQ-24, REQ-23.
