API for querying and streaming blockchain data from the SQD Network.

A portal is the HTTP gateway you query for on-chain data. You point it at a dataset (a chain), ask for
a range of blocks with the fields and filters you care about, and it streams the matching data back as
newline-delimited JSON.

## How a portal serves data

A single request can span the whole chain because the portal merges two sources for you: **archival**
history for everything up to the last few hours, and **real-time** blocks for the tip of the chain. You
query one endpoint; the portal stitches the ranges together.

```text
                                              ┌──────────────────────────┐
                                              │   real-time tip          │
                                              │   (unfinalized,          │
                                              │    can reorg)            │
                                              └──────────────────────────┘
                                                    ▲
                    ┌──────────────┐ ── recent ─────┘
   client ──HTTP──► │    Portal    │
                    └──────────────┘ ── historical ─┐
                                                    ▼
                                              ┌──────────────────────────┐
                                              │   archival               │
                                              │   (finalized,            │
                                              │    lags by hours)        │
                                              └──────────────────────────┘
```

## Finalized vs. real-time data

The two halves of that stream come with different guarantees, and the difference matters for any
consumer:

- **Archival (finalized) history** — everything except roughly the last few hours. High throughput and
  stable: these blocks are finalized and will not change.
- **Real-time tip** — the most recent blocks, served at low latency. These are **unfinalized**: when the
  chain reorganizes ("reorg"), a block you already received at the tip can be replaced.

So if you stream near the head of the chain, be ready for blocks you've already processed to be rolled
back. The next section is the protocol the portal gives you to detect that and resync safely — **read it
before you build a streaming consumer.**
