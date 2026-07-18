# ADR-003 — Real-time data is served through a streaming Portal proxy

Status: Accepted (historical, 2025-09-15); error handling amended by ADR-011

## Context

The recent-block store was split out of the Portal into a separate real-time service.
Clients still needed one Portal stream surface across archival and recent ranges, with
the same streaming and continuation behavior. Buffering a complete upstream response
would defeat streaming and amplify memory use; retrying a request after any response
bytes had arrived could duplicate a prefix.

## Decision

For a request routed to the real-time source, the Portal makes one upstream request and
streams its response to the client. It does not retry or follow redirects. Successful
responses, 204 responses, record bytes, public response headers, and status codes are
preserved; Portal-internal diagnostic headers are stripped. A failure after response
streaming begins truncates the client response on a valid record boundary under
ADR-001.

The original implementation also passed upstream error bodies through unchanged. That
part of the historical decision is amended by ADR-011: error statuses are classified
into the Portal's public taxonomy and their bodies are rewritten into the Portal error
envelope. Public upstream headers such as `Retry-After` and `x-sqd-*` remain preserved.

## Consequences

The Portal remains a low-buffering data-plane proxy, and one client request is never
replayed invisibly against the real-time service. Success-path payload fidelity is
simple to reason about. The Portal nevertheless owns its public error contract: clients
see one error vocabulary regardless of which data source served the route. Per-call
deadline requirements were added later by ADR-010.
