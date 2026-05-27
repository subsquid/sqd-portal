# Graceful shutdown

How the portal handles `SIGTERM` without dropping in-flight requests or causing
upstream LBs to route traffic to a closing listener.

## Problem

On `SIGTERM`, the tokio runtime tears down `main` and aborts all spawned tasks.
Long-lived responses (`/stream`, `/archival-stream`, `/finalized-stream`) get
dropped mid-frame, network-client cancellation never fires cleanly, and
upstream load balancers keep routing new traffic to the pod for several
seconds while their endpoint controllers catch up.

## Solution: two-phase shutdown

```text
                     SIGTERM
                        │
                        ▼
      ┌──────────────────────────────────────┐
      │  Phase 1: pre-drain                  │
      │                                      │  pre_drain_grace_period
      │  • /ready returns 503                │  (default 25 s)
      │  • all other endpoints respond 200   │
      │  • listener still accepts            │  ← LB observes 503 and pulls
      │                                      │    us out of rotation
      └──────────────────────────────────────┘
                        │
                        ▼
      ┌──────────────────────────────────────┐
      │  Phase 2: drain                      │
      │                                      │
      │  • new connections refused (listener │  bounded by
      │    closed by axum)                   │  drain_timeout
      │  • in-flight requests complete       │  (default 25 s)
      │  • HTTP/1.1: Connection: close on    │
      │    the response so keep-alive client │
      │    drops the socket                  │
      │  • HTTP/2:   GOAWAY frame            │
      └──────────────────────────────────────┘
                        │
                        ▼
                  process exits 0
```

Total app shutdown budget = `pre_drain_grace_period + drain_timeout` (default
**50 s**). The orchestrator's kill timeout (k8s `terminationGracePeriodSeconds`
or equivalent) must exceed this — recommended **≥ 60 s** to leave headroom for
network-client wind-down, Sentry flush, and runtime drop.

## Components

```text
                                         ┌──────────────────────────┐
                            spawns       │ watch_shutdown_signal()  │
   ┌──────────────────┐─────────────────▶│   (src/main.rs)          │
   │     main()       │                  │                          │
   │  (src/main.rs)   │                  │   awaits SIGTERM         │
   └──────────────────┘                  │   on receipt calls ▼     │
            │                            │                          │
            │ creates                    │  run_shutdown_sequence() │
            │                            │                          │
            ▼                            │  1. shutting_down ← true │
   ┌──────────────────┐                  │  2. sleep(pre_drain)     │
   │ shutting_down:   │ ◄────────────────│  3. cancel.cancel()      │
   │ Arc<AtomicBool>  │                  └──────────────────────────┘
   │                  │                                │
   │ cancellation:    │                                │
   │ CancellationToken│ ◄──────────────────────────────┘
   └──────────────────┘
            │                ┌────────────────────────────────┐
            │ both passed    │      run_server()              │
            ├───────────────▶│   (src/http_server.rs)         │
            │                │                                │
            │ token only     │ • /ready handler reads         │
            └───────────────▶│   shutting_down via Extension  │
                             │                                │
                ┌────────────│ • axum::serve(...)             │
                │            │     .with_graceful_shutdown(   │
                │            │        cancel.cancelled()      │
                │            │     )                          │
                │            │                                │
                │            │ • drive_serve_with_drain()     │
                │            │    races graceful drain        │
                │            │    against drain_timeout       │
                │            └────────────────────────────────┘
                │
                │ same token observed by:
                ▼
       NetworkClient::run(cancel)  ──▶  child tasks exit cleanly
       (src/network/client.rs)
```

### Why two state primitives?

- `shutting_down: Arc<AtomicBool>` — flips **at signal time** (start of
  pre-drain). `/ready` reads it on every probe (lock-free atomic load).
- `cancel: CancellationToken` — fires **after** the grace expires. Triggers
  the axum graceful shutdown AND `NetworkClient::run` wind-down.

Conflating them would either cancel network tasks too early (during
pre-drain, when we still want them serving) or keep `/ready` lying for the
entire pre-drain window.

## drive_serve_with_drain

The race between graceful drain and hard timeout:

```text
       cancel.cancel() fires
              │
              ▼
   ┌──────────────────────────────────────────────────┐
   │  tokio::select!                                  │
   │                                                  │
   │   ┌── arm 1 ───────────────────────────────┐     │
   │   │  axum::serve(...).with_graceful_       │     │
   │   │       shutdown(cancel.cancelled())     │     │
   │   │                                        │     │
   │   │  hyper sends Connection: close,        │     │
   │   │  stops accepting, drains in-flight     │     │
   │   └────────────────────────────────────────┘     │
   │                                                  │
   │   ┌── arm 2 ───────────────────────────────┐     │
   │   │  shutdown_signal.cancelled().await     │     │
   │   │  tokio::time::sleep(drain_timeout)     │     │
   │   │                                        │     │
   │   │  hard deadline; fires drain_timeout    │     │
   │   │  after cancel.cancel()                 │     │
   │   └────────────────────────────────────────┘     │
   └──────────────────────────────────────────────────┘
                │
                │  whichever resolves first wins
                ▼
        ┌──────────────┐         ┌──────────────────────┐
        │ arm 1 wins   │   OR    │ arm 2 wins           │
        │ → drained    │         │ → drain timed out    │
        │   cleanly,   │         │ → serve future       │
        │   serve      │         │   dropped → listener │
        │   future Ok  │         │   closed             │
        └──────────────┘         └──────────────────────┘
```

### What "force-close" actually does

When arm 2 wins, dropping the `serve` future:

- ✅ closes the `TcpListener` (the OS closes the accept socket)
- ❌ does **not** abort per-connection tasks

Axum 0.7 spawns each connection via `tokio::spawn`, so those tasks are
owned by the runtime, not by the `Serve` future. They keep running until
either they finish naturally or `main()` returns and `#[tokio::main]` drops
the runtime, which aborts all spawned tasks.

So `drain_timeout` bounds **time-to-`run_server`-return**, not
time-to-client-disconnect. Clients see RST ≈ `drain_timeout + ~3 s`
(network wind-down + Sentry flush + runtime drop).

## Configuration

In `mainnet.config.yml` / `tethys.config.yml`:

```yaml
pre_drain_grace_period_sec: 25   # default 25
drain_timeout_sec: 25            # default 25
```

The pre-drain default of 25 s is sized for a typical k8s readiness probe
(`periodSeconds: 5, failureThreshold: 3`):

```
worst-case time to NotReady   = periodSeconds × failureThreshold
                              = 5 × 3 = 15 s
EndpointSlice propagation     ≈ 5 s
safety margin                 ≈ 5 s
                              ───────
pre_drain_grace_period        = 25 s
```

If your probe is tuned differently, recompute:

```
pre_drain ≥ periodSeconds × failureThreshold + ~5 s propagation
```

## SIGINT (Ctrl-C) is intentionally not handled

Production never receives SIGINT (k8s sends SIGTERM only). Locally we want
Ctrl-C to terminate the process **instantly** via the default OS handler —
installing `tokio::signal::ctrl_c()` would swallow that default and force
the dev to wait through the full pre-drain + drain budget.

## Non-goals

| Behavior                                | Status     | Why |
|-----------------------------------------|------------|-----|
| Per-request `tokio::spawn` cancel       | Not done   | Plumbing a cancel token through every spawn site (stream subtasks, `spawn_blocking` for query signing, axum's per-connection tasks) doesn't scale; orphans die on runtime drop. |
| Windows compatibility (`#[cfg(unix)]`)  | Not done   | Portal deploys exclusively on Linux. Defensive code for a non-target. |
| Per-stream cooperative shutdown         | Not done   | Stream handlers are driven by hyper polling; they get dropped when the connection closes. |
| `NetworkClient` wind-down timeout       | Not done   | Expected to exit promptly on cancel; if it ever hangs, the orchestrator's kill timeout is the backstop. |
| `Datasets::update` reqwest timeout      | Pre-existing | The orphan update loop has no timeout (`src/datasets.rs`). Not affected by shutdown — detached from `main()`'s `try_join!`. Worth filing as a separate ticket. |

## Failure mode

If `network_client.run` or Sentry flush hangs past the drain budget,
spawned-but-not-cancelled tasks keep running, `main()` doesn't return,
the runtime isn't dropped, and the orchestrator sends `SIGKILL` at
`terminationGracePeriodSeconds`. **This is accepted behavior** — the portal
is stateless (no data loss), clients are designed for disconnects (stream
resume from last block), and the only operational impact is
`ExitCode=137` showing up in pod status.

If `SIGKILL` becomes frequent under load, the escalation path is adding
`std::process::exit(0)` after the force-close arm — not threading cancel
tokens through every spawn site.

## Verification

See `Verification` section in the PR description, plus the test matrix:

| Test | What |
|---|---|
| `config::tests::shutdown_durations_*` | YAML parsing with defaults and overrides |
| `tests::shutdown_sequence_flips_flag_then_cancels_after_grace` | `run_shutdown_sequence` timing on paused tokio clock |
| `http_server::tests::drive_serve_returns_ok_when_serve_finishes_before_drain_timeout` | clean drain path |
| `http_server::tests::drive_serve_force_closes_when_serve_outlasts_drain_timeout` | force-close path |
| `http_server::tests::drive_serve_propagates_serve_error` | serve error propagation |
| `http_server::tests::ready_endpoint_flips_to_503_when_shutdown_flag_set` | end-to-end HTTP: flipping `shutting_down` flips `/ready` from 200 to 503 |

For manual smoke (SIGTERM delivery, force-close path, Ctrl-C escape hatch),
see the PR description.
