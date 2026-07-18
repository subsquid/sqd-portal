# ADR-001 — Mid-stream failure truncates the response; continuation is the recovery path

Status: Accepted (historical, 2024-08-28)

## Context

Stream responses are delivered incrementally over HTTP. Once the success status and a
body prefix are sent, there is no in-band way to convert the response into an error.
The alternatives — buffering entire responses to guarantee atomicity, or a framing
protocol with in-band error records — respectively defeat the point of streaming and
break the plain line-per-record contract consumed by existing clients.

## Decision

Failures before the first record map to a proper error status. Failures after it end
the body early: always on a record boundary, always with valid encoding — a truncated
response is indistinguishable from a short successful one. The client's normal
continuation loop (REQ-2: resume from the last received block) is the recovery
mechanism; the Portal logs and counts the interruption on its side.

## Consequences

Streaming stays cheap and the client contract stays simple; every delivered prefix is
usable. The cost: a client cannot tell "range complete" from "stream broke" without
comparing its position against the head metadata — acceptable because well-behaved
clients loop on continuation anyway. Whether truncation should become explicitly
detectable (e.g. a trailing marker) is open as OQ-2. Shapes REQ-6, REQ-1.
