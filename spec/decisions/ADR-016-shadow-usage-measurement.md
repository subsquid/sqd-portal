# ADR-016 — Shadow usage measurement: encoded bytes at egress, deltas, and tolerated loss

Status: Accepted (2026-08-12)

## Context

The Portal is about to be priced, and nobody knows what it serves. There is no record of
how much data any credential consumed: `stream_bytes` is a per-stream histogram of
compressed *worker payloads* on one of the paths, keyed by dataset and attributable to
nobody, and it does not cover the real-time or SQL surfaces at all. Every pricing question
— what a heavy customer costs, what the p99 stream is worth, whether volume or request
count is the fair unit — is currently answered by guessing.

Three constraints shaped the answer.

**The measurement must be safe to turn on.** Authorization landed recently and the same
traffic pays for both. A metering path that can refuse or stall a request is enforcement
with the switch off, and the blast radius of getting it wrong is every paying stream. So
the design's first property is not accuracy, it is non-interference (INV-32).

**Streams here are long.** Continuous streams are the product; a customer opens one and
holds it for hours. Anything recorded only at completion would systematically miss the
traffic that matters most, and would report nothing at all about a portal's current hour.

**Logical bytes are expensive and premature.** What a customer intuitively "uses" is
decompressed data. Counting it on the serving path means either inflating every response —
new decompression the Portal does not do today, on the hot path, for accounting — or
threading a counter through every producer (network fan-out, real-time proxy, SQL) and
keeping four of them correct. Neither is worth doing before anyone knows whether logical
volume is the pricing unit.

## Decision

Record **encoded response-body bytes at egress**, in **delta records**, and treat loss as
acceptable and counted.

1. **One tap, at egress.** Bytes are counted where the response body is handed to the
   transport, on gated routes, labeled with the encoding actually sent. That point is
   common to every route, so hotblocks and SQL are covered by construction rather than by
   four separate integrations. Phase 2 introduces **no decompression**: no codec taps, no
   counting that requires inflating anything. (Not "the Portal never decompresses" — the
   default gzip path recompresses today — the point is metering adds none.)

2. **Logical size is a read-time estimate.** Records carry encoded bytes and the encoding;
   logical size is estimated in the analysis queries from a (dataset family, encoding)
   ratio table. That table is recalibratable at any time and applies retroactively to
   every record already stored, which a byte count baked into an event is not. Pilot
   measurements against production, 2026-08-12: gzip EVM ≈ ×6, zstd roughly double gzip
   (ethereum-mainnet 5.3–6.6 / 8.1–9.7, base-mainnet 6.2–6.5 / 12.5, solana about half
   EVM at 3.3 / 6.2). Those are 18 hand-picked fetches, not calibrated constants — they
   establish the mechanism and already prove the encoding key is mandatory, since a gzip
   ratio applied to zstd traffic halves the estimate. Recalibrate from representative
   traffic, which phase 2's own data is what makes possible.

3. **Deltas, never totals.** A response still open after P-USAGE-INTERIM is recorded then,
   and once more when it ends — at EOF or when the client goes away. Each record covers
   the bytes since the last one, so the sum of a group's records is that group's total
   with nothing double counted, and a portal's current hour is visible without waiting for
   anyone's stream to finish. It also means no record needs a stream identity, and none
   carries one.

4. **Loss is acceptable, bounded, and counted.** Records go into a bounded queue that
   drops rather than waits; delivery retries within an age bound and then drops. There is
   no spool and no disk. Totals are consequently a **lower bound with respect to loss**,
   which is why the drop counters are part of the contract (OB-14) — a lower bound whose
   gap is measured is usable; one whose gap is unknown is not.

5. **Attribution comes from the grant, and is recorded rather than acted on.** Records
   carry the key id and the organization the control plane named. No claim recorded here
   influences any authorization decision, which is exactly why reading the new claim is
   not a claims-version change. A request with no credential produces no record.

6. **The Portal's identity is not in the record.** The control plane stamps it from the
   request signature it already verifies. A field would be one a Portal could forge; the
   signature is free and cannot be.

## Consequences

Per-event byte counts are approximate in both directions and the direction differs: a
frame is counted when the body yields it, not when the socket drains it, so a connection
that dies with data buffered leaves the count above what the client received; header and
framing bytes are never counted at all, which leaves it below. Only *event loss* is
one-directional. None of this is fixable at this layer — exactness means measuring at the
socket, which the Portal does not own — so it is stated rather than papered over, and no
number from this data is quoted as revenue.

`/sql/query` returns a worker/chunk **plan**, not result data. Its records carry the
`/sql/query` route label and are excluded from data-volume analysis at read time. If SQL
data volume ever matters, that is scanned-bytes work, reopened as a stated limitation
rather than smuggled in here.

Counting costs per-frame work on every measured response. That is a budget to measure
(CT-6, CT-11), not a cost to claim is zero.

## Alternatives rejected

**Count logical bytes on the serving path.** The pricing-intuitive number, and it needs
either new decompression on the hot path or a counter in every producer. Rejected for
phase 2: it buys accuracy in a unit nobody has yet decided to price, at the cost of hot-path
work and four integrations that must each stay correct.

**Exact encoded size from the format.** gzip ISIZE trailers and the worker's zstd
`pledged_src_size` would give exact decompressed sizes with no inflation. Deferred, not
rejected: it is the upgrade path once the ratio table's error is known to matter, and it
requires worker-side cooperation this phase does not need.

**Completion-only records.** Simpler, and one record per response. Rejected: it is biased
exactly where the money is, and gives an operator nothing about traffic in flight.

**Durable spool for undelivered records.** Would make totals exact. Rejected: it turns
measurement into stateful infrastructure on a service that owns no persistent state (NG5),
to protect a number that is explicitly not a billing record in this phase.
