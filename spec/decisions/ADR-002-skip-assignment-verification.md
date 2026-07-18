# ADR-002 — Assignment is loaded without structural verification

Status: Accepted (historical, 2025-08-08)

## Context

The assignment artifact (a flatbuffer of several hundred megabytes on mainnet) was
originally parsed with full structural verification. Verification took over 90 seconds
on mainnet — paid at startup, delaying readiness, and again on every refreshed
artifact. The artifact comes from the network's own publisher over HTTPS, so transfer
corruption is unlikely; the perceived risk of a malformed artifact was judged low
against a guaranteed 90-second cost.

## Decision

Load the assignment with the unchecked constructor — no structural verification. Trust
the publisher end to end.

## Consequences

Startup and refresh are fast. In exchange, a corrupt or truncated artifact is accepted
and can fault later on any access path — panicking the refresh task or leaving the
Portal ready on garbage routing. This knowingly violates the REQ-26 intent and is
registered as GAP-1 (P1). Revisit when a cheap validation exists (bounded structural
spot-checks, a publisher-side checksum, or an upstream fix making full verification
affordable); such a change would supersede this ADR.
