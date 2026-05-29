## Blockchain forks

Public blockchains can **reorganize** ("reorg") — a block that was on the canonical chain a moment ago can be replaced
by a block from a competing branch, invalidating its descendants. A streaming consumer that has already processed a
now-orphaned block will be out of sync with the network unless the portal tells it which blocks to discard.

The portal handles this via the optional `parentBlockHash` field on a stream request and a structured **409 Conflict**
response.

> [!IMPORTANT]
> **Always pass `parentBlockHash` when resuming a stream.** A reorg can happen on any chain — you can't prevent
> conflicts, only detect them. A client that omits `parentBlockHash` will silently process blocks from a forked chain
> after a reorg, with no signal that anything is wrong.

### How parentBlockHash works

When opening a stream, the client passes the **hash of the parent of `fromBlock`** (i.e. the last block the client
already trusts). The portal compares that hash to the parent it sees for `fromBlock` on the current canonical chain:

```text
                          portal's view of the chain
        ──────────────────────────────────────────────────────────────────►

        ┌─────┐           ┌─────┐           ┌─────┐           ┌─────┐
   ...  │ N-2 │ ────────► │ N-1 │ ────────► │  N  │ ────────► │ N+1 │  ...
        └─────┘           └─────┘           └─────┘           └─────┘
                             ▲                 ▲
                             │                 │
                     parentBlockHash       fromBlock
                     from the client       from the client
```

- If `parentBlockHash` matches the parent the portal sees for `fromBlock` → the stream starts normally.
- If it does **not** match → a fork happened between the client's last seen block and `fromBlock`. The portal returns
  `409 Conflict`.

### What a conflict means

A conflict means the chain the client remembers has diverged from the canonical chain the portal sees, at or before
`fromBlock`: the client can't safely resume from this block and must back up first. The portal surfaces this conflict as
a `409 Conflict` response.

### Visualising a reorg

```text
                                ┌─────┐
                                │ N+1'│  ← new canonical tip
                          ┌────►└─────┘
                          │
        ┌─────┐   ┌─────┐ │ ┌─────┐
   ...  │ N-1 │──►│  N  │─┴►│ N+1 │     ← orphaned branch
        └─────┘   └─────┘   └─────┘       (the client may
                                            have processed
                                            this already)
```

If the client tries to resume at `fromBlock = N+2` with `parentBlockHash = hash(N+1)` (orphaned), the portal sees that
`N+1` is no longer the parent of `N+2'` on the canonical chain → it reports a conflict (`409`).

### Conflict response body

```json
{
  "previousBlocks": [
    {
      "number": 21780872,
      "hash": "0xf6a96a29..."
    },
    {
      "number": 21780871,
      "hash": "0xab12cd..."
    }
  ]
}
``` 

- `previousBlocks` is a single array of `{ number, hash }` pairs from the **current canonical chain** at and below the
  conflict point.
- The array length is arbitrary, but is **guaranteed to contain at least the parent of the requested `fromBlock`**.
- Order: most recent first (canonical chain descending from the conflict point).

### Recommended client behaviour

When the portal reports a conflict (`409`):

1. **Walk back** through `previousBlocks` and look for a block `{ number, hash }` you already trust — i.e. one whose
   `(number, hash)` you've previously processed and stored.
2. If you find a shared ancestor at block `K`, **resume the stream from `K+1`** with `parentBlockHash = hash(K)`.
3. If `previousBlocks` does not contain any block you recognise (the divergence is deeper than the array provided), *
   *re-request earlier blocks** — open a new stream that includes a `fromBlock` further in the past — and repeat the
   search. **Always pass `parentBlockHash`** on every retry too: that keeps the portal comparing your view against the
   canonical chain, so each conflict narrows the search instead of just resending the same one.

```text
   client state          portal canonical chain
   ────────────          ──────────────────────

   ✓ block K   ─── matches ───► block K          ← shared ancestor: resume here
   ? block K+1               ─► block K+1'
   ✗ block K+2 (orphaned)    ─► block K+2'
   ✗ block K+3 (orphaned)    ─► block K+3'  (tip)
```

The portal never tells you "rewind exactly to block X" — it gives you a slice of the canonical chain and trusts your
client to compare against its own state.
