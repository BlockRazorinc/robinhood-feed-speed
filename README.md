# rh-feed-speed

`rh-feed-speed` is a WebSocket block feed latency comparison tool. It connects to multiple WebSocket block sources at the same time, identifies the same block across sources, and compares their block arrival times.

For every block received by all configured sources, `rh-feed-speed` reports which source received the block first and how much later the other sources received it. The program also generates a latency percentile summary every 5 minutes and exposes JSON summary and Prometheus metrics endpoints.

## What does `rh-feed-speed` do?

`rh-feed-speed` can be used to:

- Compare block arrival times across multiple WebSocket sources.
- Identify which source received each block first.
- Measure each source’s latency relative to the winning source.
- Calculate p10, p50, p75, p90, p95, p99, and p99.9 latency percentiles.
- Count the number of wins for each source.
- View rolling 5-minute statistics through logs or JSON.
- Export source win counters in Prometheus format.

A block is included in the completed-block statistics only after it has been received by all configured sources.

## Features

- Concurrent connections to multiple WebSocket block sources.
- Configurable `-source name=url` arguments.
- Source-specific subscription messages.
- Block matching by `blockHash`.
- Fallback matching by `block:<blockNumber>`.
- Per-block latency and winner output.
- Automatic 5-minute percentile summaries.
- JSON summary endpoint.
- Prometheus metrics endpoint.
- Configurable record retention through `-dedup-ttl`.
- Debug output for message parsing errors.
- Snapshot-based summary calculation to reduce time spent holding the tracker lock.

## Startup

At least one `-source name=url` argument is required. In practice, two or more sources are usually needed to compare block arrival latency.

```bash
go run . \
  -source official=wss://feed.mainnet.chain.robinhood.com \
  -source feeder=wss://us.robinhood-feeder.blockrazor.io/ws/{authToken}
```

Each source follows this format:

```text
-source name=websocket_url
```

The source name is used in block details, latency summaries, and Prometheus metric labels.

## Viewing block latency

After startup, the program first prints a header:

```text
[block]	sequence_number	block_hash	fast	slow	winner
```

When the same block has been received by all configured sources, the program prints one detail line:

```text
[block]	12345	0xabc...	0s	18ms	fast
```

The fields mean:

| Field | Description |
|---|---|
| `sequence_number` | Sequence number parsed from the WebSocket message. |
| `block_hash` | Block hash used to identify the block. |
| Source column | Source latency relative to the first source that received the block. |
| `winner` | Source that received the block first. |

In this example, `fast` received the block first. The `slow` source received the same block `18ms` later.

## Five-minute latency summary

The program automatically outputs a latency summary every 5 minutes:

```text
[summary]	type	incremental	window	5m0s	completed_blocks	120
[summary]	type	incremental	source	fast	winner	80	p10	0s	p50	0s	p75	2ms	p90	5ms	p95	8ms	p99	20ms	p99.9	30ms	max	35ms
```

The summary contains:

| Metric | Description |
|---|---|
| `window` | Time window used for the summary. |
| `completed_blocks` | Number of blocks fully compared in the window. |
| `winner` | Number of blocks first received by the source. |
| `p10` | 10th-percentile latency. |
| `p50` | Median latency. |
| `p75` | 75th-percentile latency. |
| `p90` | 90th-percentile latency. |
| `p95` | 95th-percentile latency. |
| `p99` | 99th-percentile latency. |
| `p99.9` | 99.9th-percentile latency. |
| `max` | Maximum recorded latency. |

Each percentile value represents that source’s latency relative to the source that received the corresponding block first.

## JSON summary endpoint

Request the current summary in JSON format:

```bash
curl http://127.0.0.1:9092/summary
```

The `/summary` endpoint currently returns a 5-minute window summary using:

```go
SummaryWithPercentiles(now, 5*time.Minute)
```

## Prometheus metrics

Prometheus metrics are exposed at:

```bash
curl http://127.0.0.1:9092/metrics
```

The currently available metric is:

```text
speed_test_source_wins_total{source="xxx"} 80
```

This counter records how many fully compared blocks were first received by the specified source.

## Summary window behavior

Both the automatic 5-minute summary and the `/summary` endpoint call:

```go
SummaryWithPercentiles(now, 5*time.Minute)
```

The implementation generates the summary as follows:

1. Copy a snapshot of the current tracker records.
2. Release the tracker lock.
3. Calculate winner counts and latency percentiles from the snapshot.

This avoids holding the tracker lock during the complete summary calculation.

The window fields are calculated as follows:

- `completed_blocks` is the number of fully compared blocks in the window.
- `winner_counts[].count` is the number of wins for each source.
- `winner_counts[].percentiles` contains the latency percentiles for each source.

## Configure `-dedup-ttl`

Window statistics are based only on records still retained in the tracker. The `-dedup-ttl` setting therefore directly affects how much data the summary can inspect.

To inspect a complete 5-minute window, set `-dedup-ttl` to at least `5m`. For example:

```bash
go run . \
  -dedup-ttl 10m \
  -source official=wss://feed.mainnet.chain.robinhood.com \
  -source feeder=wss://us.robinhood-feeder.blockrazor.io/ws/{authToken}
```

With the default `30s` TTL, the 5-minute summary can only count records from roughly the most recent 30 seconds that have not already been cleaned up.

Recommended relationship:

```text
summary window: 5m
dedup TTL:      >= 5m
```

## Rolling and full summaries

The tracker supports two summary scopes:

| Method | Summary scope |
|---|---|
| `SummaryWithPercentiles(now, 5*time.Minute)` | Uses retained records from the 5-minute window. |
| `Summary(now, 0)` | Uses accumulated `WinCount` and `DelaySamples` since the process started. |

The automatic logs and HTTP `/summary` endpoint currently use the 5-minute window summary.

## Supported WebSocket message format

Incoming messages are currently parsed according to this JSON structure:

```json
{
  "version": 1,
  "messages": [
    {
      "sequenceNumber": 12345,
      "blockHash": "0xabc...",
      "message": {
        "message": {
          "header": {
            "blockNumber": 100
          }
        }
      }
    }
  ]
}
```

The parser reads these fields:

| JSON field | Usage |
|---|---|
| `messages[].sequenceNumber` | Sequence number displayed in block output. |
| `messages[].blockHash` | Primary ID used to match the same block across sources. |
| `messages[].message.message.header.blockNumber` | Fallback block number when no hash is available. |

If a WebSocket message does not contain a recognizable block, it is skipped. When `-debug` is enabled, parsing errors are printed.

## Block matching

`Tracker.RecordBlock` identifies blocks using the following order:

1. Use `blockHash` as the block ID.
2. If no block hash is available, use `block:<blockNumber>`.
3. If the message does not contain a recognizable block, skip it.

The first source to record a block becomes the winner. Delays for later sources are calculated as:

```text
now - FirstSeenAt
```

The block is marked as completed after all configured sources have received it.

## Code flow

The main program flow is implemented in `main.go`:

1. Parse source, subscription message, metrics, and TTL arguments.
2. Start one WebSocket goroutine for each source.
3. Send the corresponding `-source-subscribe` message after the WebSocket connects.
4. Parse `messages[].blockHash`, `sequenceNumber`, and `blockNumber`.
5. Pass the block to `Tracker.RecordBlock`.
6. Use the block hash as the block ID or `block:<blockNumber>` as the fallback.
7. Record the first source as the winner.
8. Calculate the delays for later sources.
9. Mark the block as completed after every source has received it.
10. Print the block details.
11. Increment the winner’s Prometheus counter.
12. Store records in a min-heap ordered by `FirstSeenAt`.
13. Index records by block ID for fast lookup.
14. Continuously remove expired records from the heap head.
15. Copy a snapshot before calculating the summary and release the lock.

## FAQ

### What is `rh-feed-speed`?

`rh-feed-speed` is a program that compares the arrival time of the same block across multiple WebSocket block sources.

### How many sources are required?

At least one source is required to start the program. Two or more sources are usually needed to compare latency.

### How is the fastest source determined?

The first configured source to receive a block is recorded as the winner. Other source delays are calculated relative to that first arrival.

### When is a block counted as completed?

A block is counted as completed after it has been received by all configured sources.

### Why does the five-minute summary contain less than five minutes of records?

The summary can only use records still retained by the tracker. If `-dedup-ttl` is shorter than five minutes, older records may be removed before the summary is calculated.

### What value should be used for `-dedup-ttl`?

Use at least `-dedup-ttl 5m` when the complete five-minute summary window is required. The original example uses `-dedup-ttl 10m`.

### What happens if a message has no block hash?

If a block number is available, the tracker uses `block:<blockNumber>` as the block ID. Otherwise, the message is skipped.

### Which Prometheus metric is available?

The current metric is `speed_test_source_wins_total`, which counts wins for each source.

### How can parsing errors be viewed?

Enable `-debug` to print message parsing errors.
