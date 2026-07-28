# ADR-016 — Effective-from gates activation; the publisher controls assignment selection

Status: Accepted (2026-07-28); supersedes ADR-014 decision 3

## Context

ADR-014 decision 3 treated an assignment's effective-from timestamp as a revision and
required artifact application to be monotone in that value. That interpretation was
not part of the scheduler/publisher contract: effective-from says when an artifact may
be activated, not whether it supersedes another artifact.

The monotonic guard also prevented recovery. If the publisher selected a previously
valid artifact to roll back a bad assignment, its earlier effective-from caused the
Portal to reject the publisher's current state. This moved scheduler policy into the
Portal and made an intentional rollback indistinguishable from stale publication.

## Decision

The publisher's currently selected assignment identifier is authoritative.
Identifiers are opaque and compared only for equality to avoid re-downloading the
already applied artifact. Effective-from is only an activation gate: the Portal waits
until that time, but never uses it to order artifacts.

A different identifier is fetched, validated, and atomically applied after its
effective-from even when that timestamp predates the applied artifact's. Fetch or
validation failure still leaves the applied artifact untouched.

## Consequences

The publisher can roll routing back to a previously valid assignment. The Portal no
longer reports or rejects a `regressive` refresh, because no such ordering exists in
its contract.

This removes Portal-side protection against an accidentally republished old
identifier. If the scheduler needs replay protection, its contract must provide an
explicit monotone revision or another signal that distinguishes an accident from an
intentional rollback; effective-from cannot encode both meanings.

DEF-4, INV-2, DC-2, REQ-40, and the publisher failure model are updated accordingly.
