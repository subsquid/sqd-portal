# Portal conformance harness (Phase 0)

The harness from `spec/13-conformance.md`: the portal runs as a black-box child
process against stubs for every dependency in `spec/05-dependencies.md`, wired
per the input-side binding IB-7. Stubs record everything in ledgers — the
ground truth the oracle and validators check against.

What exists (Phase 0, `spec/13` §build order):

- **Stubs with ledgers** — assignment publisher (network-state JSON + a real
  gzipped FlatBuffer artifact built with `sqd-assignments`), dataset registry,
  real-time source (heads/status/stream, emits `x-internal-*` noise to prove
  stripping), chain contracts (the contract client's dummy-data file), two
  **worker stubs speaking the actual p2p query protocol** on the portal's
  pinned transport rev, answering signed `QueryResult`s, and a **control plane**
  (DC-8) that verifies the exchange signature against the portal's own identity
  key rather than trusting the headers.
- **Toy world** — deterministic datasets/blocks/chunks every stub serves from.
- **Reference model** — the oracle of `spec/13`, specialized to the toy world.
- **Structural validators** — five run on every stream response; the sixth
  (error-envelope) is defined but not yet exercised by the CT-1 smoke.
- **Client driver** — raw status/headers/bytes, decoding per declared encoding.
- **Quiescence-gated gauge audit** — INV-30 at rest.

Run it:

```sh
cargo build                 # in the repo root: the harness spawns target/debug/sqd-portal
cd harness && cargo test
```

`PORTAL_BIN=/path/to/sqd-portal` overrides the binary. The smoke needs no
network access and finishes in seconds.

`ct10_authorization` is the authorization class. It boots four portals, because
the properties differ by configuration rather than by request: enforcing, shadow
(`log_only`), one with no `auth:` block at all, and one whose exchange
budget is small enough to saturate under a burst. Its load-bearing assertions are
the ones a response alone cannot make — that a refused request reaches no worker,
no real-time source and no control plane; that a burst of eight on one credential
costs one exchange; and that no presented secret appears anywhere in the portal's
log or its keyless scrape. The last one guards itself: it fails if no credential
ever travelled, or if the log and scrape it greps carry no authorization activity.

Notes for future phases: the two-worker toy network is deliberate — the portal
pre-leases `1 + retries` *distinct* workers per chunk, and with two workers the
readiness gate (`⌊2·¾⌋ = 1`) requires a live p2p connection. The harness config
shortens penalty windows to 1s; penalty semantics belong to CT-2, which should
pin them explicitly.
