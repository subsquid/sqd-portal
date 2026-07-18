# ADR-012 — Every shed response carries an explicit retry hint

Status: Accepted (2026-07-17)

## Context

In the 2026-07 production incident, a saturated public portal hitting its stream cap
returned 503 with **no** `Retry-After` and a message blaming the network ("no available
workers"). The dominant client SDK honors a pause only when `Retry-After` is present;
otherwise it falls back to its own schedule whose first step is 10 ms. Shed load came
straight back — a self-amplifying refusal storm measured at ~314 rps — and the
misattributed message sent operators debugging the wrong layer. (The same event
correlates with OOM-kill restarts tracked separately as GAP-3.)

## Decision

Split the old catch-all "unavailable" error into two: **stream-cap exhaustion** returns
529 with `Retry-After: 1` (P-RETRY-AFTER-MIN), and **no-worker-holds-the-data** keeps
503 with no hint. Every overload-class response (cap, rate-limit, bandwidth busy)
carries an explicit `Retry-After`; busy-for-a-duration responses hint that duration
rounded up. Error text names the portal, not the network, as the refusing party.

## Consequences

Load shedding actually sheds: conforming clients hold off for the hinted interval, so
refusal work decays instead of amplifying. Clients that ignore `Retry-After` still see
the cheap refusal path. Attribution is honest, which changes operator response
(scale/stake up vs. investigate the network). Shapes REQ-20; extends ADR-007.

**Implementation status:** not yet integrated on current master — the stream-cap path
still returns bare 503 with no `Retry-After`. Tracked as GAP-4 (P1).
