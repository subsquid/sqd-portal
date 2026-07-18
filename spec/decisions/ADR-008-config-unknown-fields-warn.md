# ADR-008 — Unknown config fields warn, don't reject

Status: Accepted (historical, 2026-06-08; rationale reconstructed)

## Context

Config schema evolves across releases while deployments roll: a config file may carry
keys a given binary doesn't know yet (written for a newer version) or no longer knows
(left over from an older one). Rejecting unknown keys turns every such mismatch — and
every typo — into a startup failure, i.e. a config push into an outage. (Rationale
reconstructed from the change itself — inference.)

## Decision

Unknown configuration fields are logged as warnings, naming the ignored key path, and
startup proceeds. Invalid **values** of known fields remain fatal at startup.

## Consequences

Config pushes are forward/backward compatible across rolling deploys. The cost is that
typos survive silently unless someone reads the warnings — observed in practice: a
shipped config carries stale keys from a renamed section, silently ignored. Operators
must treat the unknown-field warning as actionable. Shapes REQ-33.
