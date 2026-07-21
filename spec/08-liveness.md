# 08 — Liveness

## §0 Environmental definitions

Liveness claims hold only under a declared environment:

- **Healthy workers:** for every requested chunk, at least one assigned worker answers
  correct responses within P-TRANSPORT-TIMEOUT and is not rate-limiting.
- **Healthy publisher/registry:** DC-2/DC-3 fetches succeed within their deadlines.
- **Healthy real-time source:** DC-4 answers within its deadlines.
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

**LIV-5 — Startup bound, accept/ready decoupled.** From process start with healthy
publisher: the listener accepts connections early (probes answerable while loading),
and readiness is achieved within P-STARTUP-BOUND ⚠, dominated by the artifact
download. Startup never blocks on the real-time source or chain RPC. Witness: OB-8
lifecycle timestamps. Check: CT-2 cold-start scenario (S5).

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

**LIV-12 — No silent infinite retry.** Any divergence converges or alarms: chunk
attempts are bounded by 1 + retries, then surface RETRIES-EXHAUSTED (WORKER-FAILURE
for integrity exhaustion, DC-1) before the first record or truncation after it; refresh loops that fail
persistently raise the OB-9 alarm state (⚠ artifact case pending ADR-013); nothing
retries forever without an externally visible signal. Witness: OB-9. Check: CT-2 —
permanent-failure stubs; assert bounded attempts + alarm.
