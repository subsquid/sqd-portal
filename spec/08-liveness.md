# 08 — Liveness

## §0 Environmental definitions

Liveness claims hold only under a declared environment:

- **Healthy workers:** for every requested chunk, at least one assigned worker answers
  correct responses within P-TRANSPORT-TIMEOUT and is not rate-limiting.
- **Healthy publisher/registry:** DC-2/DC-3 fetches succeed within their deadlines.
- **Healthy real-time source:** DC-4 answers within its deadlines.
- **Healthy control plane:** DC-8 exchanges succeed within their deadline, and the absolute
  skew between the Portal's clock and the control plane's is at most
  P-SIGNATURE-MAX-SKEW — a skew past it in either direction fails every exchange, not a
  share of them (ADR-018). Vacuous on a Portal with no commercial
  configuration.
- **Adequate resources:** census below P-MAX-STREAMS, congestion utilization below
  P-HEADROOM-THRESHOLD, memory within P-MEMORY-BUDGET.
- **Patient supervisor:** the orchestrator's kill grace P-KILL-GRACE exceeds
  P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT. Below it the process is killed mid-drain, and no
  shutdown bound the Portal can offer holds.
- **Draining client:** the client consumes the response at least as fast as it is
  produced (streams are client-paced; no liveness bound holds against a stalled
  reader).
- **Quiescent:** no in-flight requests, no pending input events, and one
  P-HEARTBEAT-INTERVAL with no observable state movement (operationalized by the
  harness in 13 §harness: stub queues empty ∧ no gauge movement).

Each property: *pre* (environment) → *bound* → *witness* (OB-n,
[12-observability.md](12-observability.md)) → check (CT-n).

**LIV-1 — First-record bound.** Healthy workers/source, adequate resources, admitted
stream over available data ⇒ the first record (or a terminal error) is produced within
P-STALL-BUDGET ⚠ — the healthy-workers environment guarantees only *one* healthy
worker per chunk, and FV-1 permits an honest mis-pick, so the worst case is one worker
timeout plus a rerouted attempt (the same worst case as LIV-2). Witness: OB-2
(time-to-first-byte). Check: CT-1/CT-6.

**LIV-2 — Stream progress (stall budget).** Healthy environment, draining client ⇒ a
stream's coverage advances; zero-progress intervals never exceed P-STALL-BUDGET ⚠
(worst honest case: a worker timeout plus rerouted attempt). A stream that cannot
progress within the budget terminates (truncation or error) rather than hanging.
Witness: OB-2 heartbeat vs coverage. Check: CT-2 — stub-induced stalls.

**LIV-3 — Non-stream termination.** Every non-stream operation (OP-2..OP-10) answers
within its dependency deadline plus slack; operations with no dependency (OP-8, OP-9)
answer promptly always — including under full stream saturation. Witness: OB-3 latency
by endpoint. Check: CT-6 under S3.

**LIV-4 — Beyond-frontier bound.** A beyond-frontier request answers EMPTY in
P-NO-DATA-DELAY + slack — bounded above as well as below (the throttle must not hold
connections indefinitely). Witness: OB-3. Check: CT-1 timing.

**LIV-5 — Startup bound, accept/ready decoupled.** From process start with a healthy
publisher the listener accepts connections early (probes answerable while loading).
Readiness is achieved within the artifact fetch and apply time plus scheduling slack, which
a supported deployment sizes to ≤ P-STARTUP-BOUND ⚠. Commercial configuration adds no term:
there is no key bootstrap, and a Portal whose control plane is unreachable reaches
readiness on the same schedule as one whose control plane is healthy (REQ-54). Startup
never blocks on the real-time source, the control plane, or chain RPC. Witness: OB-8
lifecycle timestamps. Check: CT-2/CT-10 cold-start scenario (S5), including a cold start
against an unavailable control plane.

**LIV-6 — Artifact convergence.** A newly published artifact (effective time passed)
is applied within P-ASSIGNMENT-REFRESH + fetch time; routing reflects it for all new
streams thereafter. Witness: OB-6 artifact identifier/age. Check: CT-2 publisher-stub
rotation.

**LIV-7 — Penalty decay.** A worker that recovers is re-eligible within its cooldown
window (P-WORKER-ERROR-COOLDOWN / P-WORKER-TIMEOUT-COOLDOWN) — penalties never
permanently shrink the pool; a fully *penalized* (cooldown) pool still serves, because
penalized workers remain last-resort candidates. Distinct case: a pool whose every
candidate is under worker-requested backoff longer than P-MAX-IDLE-TIME refuses as
OVERLOADED instead (DC-1). Witness: OB-4 per-class selection counters. Check:
CT-2 — fail-then-recover stub worker; assert reuse.

**LIV-8 — Window recovery.** After congestion ends, the window regrows from the floor
to sustained-load equilibrium within a bounded number of successful downloads
(additive increase ⇒ linear in the deficit). Witness: OB-10 window gauge. Check:
CT-6 — congestion pulse, measure recovery.

**LIV-9 — Shed-and-recover.** When load falls below capacity, admission resumes within
P-RETRY-AFTER-MIN + slack: refusal is a function of current capacity, never a latched
state. Witness: OB-3 refusal counters returning to zero. Check: CT-6 — load step-down.

**LIV-10 — Disconnect reclamation.** A client disconnect releases everything the
stream held — census slot, leases, permits, upstream requests — within
P-TRANSPORT-TIMEOUT (no orphaned work outlives its request beyond the in-flight
attempt). Witness: OB-1 gauges at quiescence. Check: CT-3 — disconnect storm, then
INV-30 audit.

**LIV-11 — Shutdown bound.** SIGTERM ⇒ readiness flips immediately; process exits
within P-PRE-DRAIN-GRACE + P-DRAIN-TIMEOUT + slack, regardless of client behavior
(ADR-005). Witness: OB-8/OB-5. Check: CT-2 — shutdown under load with stalled readers.

**LIV-13 — Revocation convergence.** Commercial deployment ⇒ a key revoked at the control
plane stops being served by a replica within its grant's `refresh_after` + one exchange +
P-GRANT-REFRESH-JITTER, and unconditionally at that grant's `expires_at`. The same bound
covers every narrowing of a live key — a withdrawn dataset, a reduced scope — since all of
them reach the Portal only as the next grant. The first bound
needs a healthy control plane and a request to arrive — an idle credential converges
trivially, since nothing is being served on it. The second needs nothing at all: it holds
through an outage, which is what makes the stale-authorization window a number rather than
a hope, and it is capped for the fleet at P-GRANT-MAX-LIFETIME (REQ-54). Convergence is per
replica and needs no coordination; two replicas may sit a refresh apart, bounded by the same
expiry (INV-15). Nothing here converges faster than the control plane asked for; if a
product requirement needs it to, that is an invalidation channel rather than a shorter
interval (OQ-16). Witness: enforcing-mode OB-13 grace count, minimum remaining expiry, and
exchange outcomes; shadow-mode and per-request details remain protected. Check: CT-10 —
revoke a key while the stub is healthy and assert the first request past `refresh_after`
converges; repeat with the stub unreachable and assert convergence exactly at `expires_at`.

**LIV-14 — New-key admission.** Healthy control plane ⇒ a key minted a moment ago is served
on its first request: there is no set to be absent from, only an exchange to make, so
admission latency for a fresh key is one exchange (DC-8) and never a sync interval. The
bound holds only within the exchange budget — P-GRANT-EXCHANGE-RATE and
P-GRANT-EXCHANGE-INFLIGHT — and outside it the key is refused as OVERLOADED with a retry
hint rather than delayed or mislabeled as an invalid credential (HZ-10). A failed exchange
is UPSTREAM-FAILURE and likewise retryable. Witness: protected OB-13 exchange events plus
the control-plane stub ledger. Check: CT-10 — present a key the stub minted after the
replica started; assert it is served, that budget saturation returns OVERLOADED, and that a
control-plane failure returns UPSTREAM-FAILURE before the same credential succeeds after
recovery.

**LIV-12 — No silent infinite retry.** Any divergence converges or alarms: chunk
attempts are bounded by 1 + retries, then surface RETRIES-EXHAUSTED (WORKER-FAILURE
for integrity exhaustion, DC-1) before the first record or truncation after it; refresh loops that fail
persistently raise the OB-9 alarm state (⚠ artifact case pending ADR-013); nothing
retries forever without an externally visible signal. Witness: OB-9. Check: CT-2 —
permanent-failure stubs; assert bounded attempts + alarm.
