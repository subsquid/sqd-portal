# Stream fan-out efficiency

Where worker queries and downloaded responses become unnecessary. This is the operator's
guide to the OB-16 families (`spec/12-observability.md`): what they count, what they do
not claim, and the queries that turn them into questions worth investigating about
read-ahead, hedging, retries and cancellation.

A stream reserves `1 + retries` workers per chunk, sends one query, and races a hedge
against any query that outlives an adaptive estimate. It also fetches chunks ahead of the
client. Each of those is a policy that can waste worker time and Portal bandwidth, and
each wastes it in a way these families tell apart.

All names below carry the registry prefix as exported on `/metrics`: `portal_`. Counters
carry the `_total` suffix.

## The families

| Family | Labels | One increment is |
|---|---|---|
| `portal_stream_queries_sent_total` | `dataset`, `kind` | a query handed to the transport, after its congestion permit |
| `portal_stream_queries_withdrawn_total` | `dataset`, `kind` | a query cancelled while still waiting for that permit; no worker saw it |
| `portal_stream_queries_settled_total` | `dataset`, `kind`, `outcome`, `stage`, `completion` | a sent query the controller has let go of, once |
| `portal_stream_worker_bytes_total` | `dataset`, `kind`, `outcome`, `stage`, `completion` | bytes the Portal read from that query's response, charged when it settles |
| `portal_stream_chunks_settled_total` | `dataset`, `outcome` | a chunk the stream dispatched a query for, once, when the stream is done with it |

`dataset` is the configured name, as on `http_status`. The other labels are drawn from
fixed vocabularies, so cardinality is a constant per configured dataset. No family
carries a worker or request id.

Every sent query settles exactly once, so at any window longer than a query's lifetime,
`sent ≈ Σ settled` per dataset and kind. Every byte read lands under exactly one
`(outcome, stage, completion)` triple. Every chunk a query was dispatched for settles
once, so `chunks settled ≈ first sent + first withdrawn`.

## Vocabulary

**`kind`, why the query was sent:**

| Value | Meaning | Policy behind it |
|---|---|---|
| `first` | the first query for a chunk's range | read-ahead depth (`buffer_size`) |
| `continuation` | the first query for the rest of a range a worker answered in part | worker response limits |
| `retry` | sent after every earlier attempt at the range had failed | `retries` |
| `hedge` | sent while an earlier attempt at the same range was still in flight | the adaptive timeout (`timeout_quantile`) |

**`outcome`, why the controller let go of the query:**

| Value | Meaning |
|---|---|
| `delivered` | its payload was handed to the response body |
| `failed` | the controller read its result and rejected it: an error, or an answer outside the queried range |
| `superseded` | another attempt's answer was used for the range; this one was let go without being read |
| `cancelled` | the range settled on another attempt's terminal error; this one was let go without being read. Nobody won |
| `abandoned_error` | the stream had yielded an error, and this query was still in hand |
| `abandoned_unknown` | the consumer dropped a stream that had not ended, with this query still in hand |

There is no `abandoned_disconnect`. The controller sees its own output and its own drop,
nothing more. A client disconnect is the usual reason a healthy stream is dropped, but
the Portal cannot establish it, and a label that guessed would be read as a measurement.

**`stage`, whether the controller had taken the task's result when it let go:**

| Value | Meaning |
|---|---|
| `read` | the controller had polled the task and taken what it returned |
| `in_flight` | it had not; from where the controller stood, the query was still running |

**`completion`, what the query task had done by then:**

| Value | Meaning |
|---|---|
| `ok` | the task returned an answer |
| `error` | the task returned an error |
| `incomplete` | the task had not returned; it was aborted in flight |

Three facts, recorded by two parties. The outcome and the stage are the controller's:
what it decided, and what it held. The completion is the task's: what it had actually
done, which the controller may never have looked at. They are kept as separate labels
because folded into one word they lie. A hedge that lost is `superseded` either way;
only the completion says whether a whole answer was downloaded and thrown away (`ok`),
it had already failed (`error`), or it was cut off (`incomplete`). And the completion
alone cannot say whether an abandoned answer had been buffered: a task can answer after
the controller has let its query go and before the abort lands, or answer while the
controller is between polls. Only the stage says whether the controller ever held it.

**Reachable combinations and what each one costs:**

| `outcome` | `stage` | `completion` | Reading |
|---|---|---|---|
| `delivered` | `read` | `ok` | useful work. The only triple that is |
| `failed` | `read` | `error` | a worker or transport failure the controller acted on: rerouted or given up |
| `failed` | `read` | `ok` | an answer the controller rejected (wrong range, bad signature): downloaded, unusable |
| `failed` | `read` | `incomplete` | the query task itself died (panic); should be zero |
| `superseded` | `in_flight` | `ok` | a whole answer downloaded and never read, because another attempt's was used first |
| `superseded` | `in_flight` | `error` | had failed by the time the other attempt won. A failure's cost, not a wasted answer |
| `superseded` | `in_flight` | `incomplete` | cut off mid-body because the other attempt won. The bytes are what it had pulled |
| `cancelled` | `in_flight` | any | let go because the range failed terminally on another attempt; its own result did not matter |
| `abandoned_*` | `read` | `ok` | a completed answer sitting in the read-ahead buffer when the stream ended: fetched, never sent |
| `abandoned_*` | `in_flight` | `ok` | answered, but the controller never took it: it landed after the last poll, or after the stream ended. Bandwidth spent on nothing |
| `abandoned_*` | `in_flight` | `incomplete` | still downloading when the stream ended: bandwidth and worker time spent on nothing |
| `abandoned_*` | `in_flight` | `error` | a failure nobody was left to act on |

The stage of a delivered or failed query is always `read`; of a superseded or cancelled
one always `in_flight`; and a query read and still held when the stream ended had
answered, since a read error is failed on the spot. Only those triples exist on
`/metrics`.

**`stream_chunks_settled` outcomes:** `delivered` (all of the chunk's parts reached the
body), `failed` (the stream ended on this chunk's error), `abandoned_error` and
`abandoned_unknown` (as above, per chunk rather than per query).

## What the numbers mean, and do not

- **Sent is submission, not execution.** A query is sent once it is past the congestion
  permit and handed to the transport. Nothing here proves a worker received it, ran it,
  or answered. Sent is the Portal's demand on the network. Worker-side execution is only
  visible in the worker's own metrics.
- **Bytes are application reads.** They are what the Portal pulled off the response
  stream. Data the transport had buffered when the read stopped is not counted. Nothing
  of the worker's CPU, storage or egress is counted: a query the worker fully executed
  but the Portal aborted after one byte costs the worker its whole run and shows here as
  one byte.
- **Delivered is hand-off, not receipt.** A payload is delivered when the controller
  yields it to the response body. Compression, HTTP framing and the connection sit
  between that and the client. A client that disconnects after the last hand-off leaves
  that payload delivered and the next one abandoned.
- **Worker bytes are not client egress.** `stream_worker_bytes` counts responses as the
  worker compressed them, read whole. Client egress (the usage measurement, REQ-60/61)
  counts what the Portal encoded on the wire after its own compression and framing. The
  ratio between them is a compression ratio, not a waste ratio, and neither converts to
  the other.
- **Completion is the task's final word; stage is the controller's.** `completion="ok"`
  says the task returned an answer at some point, including after the controller had
  let it go. Whether that answer was ever in the controller's hands is `stage`. Use the
  stage to tell buffered read-ahead from outstanding work, never the completion.
- **Withdrawn is not sent.** A query cancelled while queued for a congestion permit cost
  no worker anything. It appears only in `queries_withdrawn`; it does not settle, and it
  has no bytes.
- **Series start at zero from a dataset's first stream.** A rare outcome's first
  occurrence is visible to `rate()` except when it happens on that very first stream,
  before the next scrape.
- **Settled lags sent.** A query settles at the end of its life. Ratios of settled to
  sent must use windows well above a query's lifetime, or hedges still in flight read as
  losses. One hour is a safe default for win rates.

## Recipes

All by dataset. Drop the `by (dataset)` for fleet totals, or add `kind` where noted.

### Unused bytes per second

Every byte read that did not reach the response body:

```promql
sum by (dataset) (
  rate(portal_stream_worker_bytes_total{outcome!="delivered"}[5m])
)
```

### Unused-byte fraction

```promql
sum by (dataset) (rate(portal_stream_worker_bytes_total{outcome!="delivered"}[5m]))
/
sum by (dataset) (rate(portal_stream_worker_bytes_total[5m]))
```

A fraction says how much of what was read was useful. It says nothing about how much was
read: a policy change can lower the fraction while raising total bytes and total
queries. Always read it next to the absolute series in the next recipe.

### Absolute traffic

The denominators everything else should be read against:

```promql
# queries handed to the transport, by why
sum by (dataset, kind) (rate(portal_stream_queries_sent_total[5m]))
# everything read from workers, useful or not
sum by (dataset) (rate(portal_stream_worker_bytes_total[5m]))
# useful throughput: bytes that reached the response body
sum by (dataset) (rate(portal_stream_worker_bytes_total{outcome="delivered"}[5m]))
```

### Request amplification

Queries sent per chunk delivered. Above one is expected: a chunk answered in parts takes
a continuation per part.

```promql
sum by (dataset) (rate(portal_stream_queries_sent_total[5m]))
/
sum by (dataset) (rate(portal_stream_chunks_settled_total{outcome="delivered"}[5m]))
```

Extra queries per range dispatched, which isolates the two policies that add queries to
a range:

```promql
sum by (dataset, kind) (rate(portal_stream_queries_sent_total{kind=~"retry|hedge"}[5m]))
/ on (dataset) group_left
sum by (dataset) (rate(portal_stream_queries_sent_total{kind=~"first|continuation"}[5m]))
```

### Where the unused bytes go: the partition

`outcome` partitions every byte read exactly once, so this is the one breakdown that can
be stacked to a total:

```promql
sum by (dataset, outcome) (rate(portal_stream_worker_bytes_total[5m]))
```

| Slice | The policy it charges |
|---|---|
| `failed` | retries: what the controller read and rejected |
| `superseded` | hedging: attempts that lost a race |
| `cancelled` | error handling: attempts let go when a range failed elsewhere |
| `abandoned_error`, `abandoned_unknown` | read-ahead: work in hand when the stream ended |

Each slice can be refined by `stage` and `completion` without breaking the partition,
because the refinement is nested inside the slice:

```promql
# within superseded: answers thrown away vs bodies cut off vs failures, and who lost
sum by (dataset, kind, completion) (
  rate(portal_stream_worker_bytes_total{outcome="superseded"}[5m])
)
```

`kind="hedge"` is the hedge losing; any other kind is the original losing to a hedge.

### Cross-cutting views

These cut across the partition and must not be added to it or to each other. Each
answers one question on its own.

Failures wherever they landed, whether or not anyone acted on them:

```promql
sum by (dataset, outcome) (rate(portal_stream_worker_bytes_total{completion="error"}[5m]))
```

This overlaps `failed` (the ones acted on), `superseded`, `cancelled` and `abandoned_*`
(the ones nobody read). It is the failure rate as the network produced it; the partition
slice `failed` is the failure rate as the controller consumed it.

Every byte a given kind of query cost, whatever became of it:

```promql
sum by (dataset, kind) (rate(portal_stream_worker_bytes_total[5m]))
```

### Abandoned work, explained

Read-ahead the client never took, split by what state it was in and why the stream
ended:

```promql
sum by (dataset, outcome, stage) (
  rate(portal_stream_worker_bytes_total{outcome=~"abandoned_.*"}[5m])
)
```

Read it as a two-by-two:

| | `stage="read"` | `stage="in_flight"` |
|---|---|---|
| `abandoned_unknown` | answers buffered for a client that left. Read-ahead deeper than clients' patience | downloads outstanding for a client that left. Same, plus the worker's whole run |
| `abandoned_error` | answers buffered behind a chunk that failed. Read-ahead deeper than the failure rate allows | downloads outstanding behind a failed chunk |

Both rows are read-ahead; the columns say whether the controller had the answer in hand.
The `unknown` row is a question about client behaviour (short-lived streams, pagination
that stops early). The `error` row is a question about reliability: fix the failures
and this row goes with them.

The `in_flight` column splits further by `completion`: `ok` is an answer that landed
with nobody to take it, `incomplete` a download cut off, `error` a failure unread. All
three are bandwidth spent on nothing; only the first also spent it to the end.

### Datasets wasting the most read-ahead

By chunks rather than bytes, so a dataset with small chunks is not hidden:

```promql
topk(10,
  sum by (dataset) (rate(portal_stream_chunks_settled_total{outcome=~"abandoned_.*"}[1h]))
  /
  sum by (dataset) (rate(portal_stream_chunks_settled_total[1h]))
)
```

### Hedge win rate, and what to check before reading it

Hedges whose answer was the one delivered, over hedges sent:

```promql
sum by (dataset) (rate(portal_stream_queries_settled_total{kind="hedge",outcome="delivered"}[1h]))
/
sum by (dataset) (rate(portal_stream_queries_sent_total{kind="hedge"}[1h]))
```

The rate alone does not say why hedges win or lose. Read it with where the rest of the
hedges went:

```promql
sum by (dataset, outcome, completion) (
  rate(portal_stream_queries_settled_total{kind="hedge",outcome!="delivered"}[1h])
)
```

Hypotheses a low win rate is consistent with, and the series that separates them:

- The timeout fires too early and the original answers first. Consistent with hedges
  `superseded` with `completion="ok"` or `"incomplete"`. Not proof: the original may
  have been slow and the hedge slower still.
- Hedges are failing. Consistent with hedges `failed`, or `superseded` with
  `completion="error"`.
- Hedges are being abandoned. Consistent with hedges `abandoned_*`: the stream ended
  before either attempt answered.

A high win rate says hedges are the answer used. It does not say the originals were
stuck, or how much latency each win saved: there is no per-attempt latency here, so the
value of hedging has to be read from the throughput and TTFB histograms below.

### Queries cancelled before any worker saw them

```promql
sum by (dataset, kind) (rate(portal_stream_queries_withdrawn_total[5m]))
```

A withdrawn query was itself still queued for a congestion permit when cancelled. That
is all it establishes. A withdrawn hedge does not show the original was also queued; to
test that hypothesis, read this against the congestion window and in-flight gauges
(`portal_congestion_window`, `portal_congestion_in_flight`) over the same window.

## Comparing the wake-up fix

The fix (`fix(stream): take a query's answer when it arrives`) polls a query as soon as
it is sent, so a hedge that answers first can win instead of waiting to be read behind
the query it raced. Before it, answers were read in request order, and a fast hedge's
whole download was thrown away. The adaptive hedge threshold is learned from how long
answers take to be *read*, and those late reads inflated it, so the fix is also expected
to lower the threshold and raise the hedge rate.

These are hypotheses about what should move. None of the series below proves the fix
was worth it on its own; read them together, on either side of the deploy.

**Expected to fall:** answers downloaded whole and thrown away.

```promql
sum by (dataset) (rate(portal_stream_worker_bytes_total{outcome="superseded",completion="ok"}[1h]))
```

The floor is what true ties produce: both answers landing between two polls.

**Expected to rise:** the hedge rate, and with it the originals cut off by a winning
hedge.

```promql
sum by (dataset) (rate(portal_stream_queries_sent_total{kind="hedge"}[1h]))
/ sum by (dataset) (rate(portal_stream_queries_sent_total{kind=~"first|continuation"}[1h]))

sum by (dataset) (rate(portal_stream_worker_bytes_total{kind!="hedge",outcome="superseded",completion="incomplete"}[1h]))
```

**Cost, in absolute terms.** A lower unused-byte fraction does not establish a lower
total cost. If the hedge rate rises enough, total queries sent and total bytes read can
rise while the fraction falls, and the workers see more load, not less. Compare
absolute traffic (the recipe above), request amplification, and useful throughput
(delivered bytes per second) as a set. The fix is a net win on cost only if useful
throughput holds or rises while total bytes read and queries sent do not rise more than
it does. If the hedge rate rises and the hedge win rate falls, the threshold is now
firing on queries that were about to answer; `timeout_quantile` is the lever. The fix
changes when answers are read, not that policy.

**Latency.** The fix delivers a range as soon as the fastest attempt answers, so the
client-visible effect, if any, is in time to first byte and per-stream throughput on the
ranges hedging touched. Two histograms show that, neither per attempt:

```promql
histogram_quantile(0.5, sum by (dataset, le) (
  rate(portal_http_seconds_to_first_byte_bucket{endpoint=~"/stream|/finalized-stream|/archival-stream"}[1h])
))

histogram_quantile(0.5, sum by (dataset_name, le) (
  rate(portal_stream_blocks_per_second_bucket[1h])
))
```

The whole-query histogram, `portal_query_durations_seconds`, is not a control for
worker behaviour and should not be read as one. It includes the Portal's own response
reads, which wait on congestion permits, so it moves with Portal-side load. It admits
only successful queries, and the fix changes which queries succeed: it cancels slower
attempts earlier, so the population entering the histogram is different on either side
of the deploy. A shift in it after the fix is expected from selection alone.

**Method.** Annotate the deploy. Compare the same dataset over the same hours on either
side, normalising by `first` queries sent so a change in load does not read as a change
in efficiency. Use ratios next to the absolute series they are ratios of. Use windows of
an hour or more for anything with `settled` in the numerator.

## What is not measured here

- Worker-side cost of a query: execution time, storage reads, the bytes a worker
  produced that the Portal never read.
- Client receipt of a delivered payload.
- Per-attempt latency, and so the latency a winning hedge saved. The hedge threshold
  itself and the durations it is learned from are not exported; the throughput and
  TTFB histograms above are the proxy.
- Why a healthy stream was dropped. A `disconnect` value would need the HTTP layer to
  establish it, which it cannot today.
