# rh-feed-speed

`rh-feed-speed` connects to multiple websocket block sources at the same time and compares the arrival time of the same block across different sources. The program prints latency details for each fully compared block and outputs a latency percentile summary every 5 minutes.

## Startup

At least one `-source name=url` argument is required. In practice, two or more sources are usually needed to compare latency.
example:

```bash
go run . \
  -source official=wss://feed.mainnet.chain.robinhood.com \
  -source feeder=wss://us.robinhood-feeder.blockrazor.io/ws/{authToken}
```
## Viewing latency

After startup, the program first prints a header line:

```text
[block]	sequence_number	block_hash	fast	slow	winner
```

When a block has been seen by all configured sources, the program prints one detail line:

```text
[block]	12345	0xabc...	0s	18ms	fast
```

Each source column shows that source's latency relative to the first source that received the block. `winner` is the source that saw the block first.

The program automatically outputs a summary every 5 minutes:

```text
[summary]	type	incremental	window	5m0s	completed_blocks	120
[summary]	type	incremental	source	fast	winner	80	p10	0s	p50	0s	p75	2ms	p90	5ms	p95	8ms	p99	20ms	p99.9	30ms	max	35ms
```

You can also request a JSON summary manually:

```bash
curl http://127.0.0.1:9092/summary
```

Prometheus counters are exposed at `/metrics`:

```bash
curl http://127.0.0.1:9092/metrics
```

Currently there is only one Prometheus metric:

```text
speed_test_source_wins_total{source="xxx"} 80
```

## Summary window behavior

Both the automatic 5-minute summary and `/summary` call `SummaryWithPercentiles(now, 5*time.Minute)`. The implementation first copies a snapshot of the current records in the tracker, then releases the lock and calculates the summary from that snapshot:

- `completed_blocks`: the number of fully compared blocks in the window.
- `winner_counts[].count`: the number of wins for each source in the window.
- `winner_counts[].percentiles`: the latency percentiles for that source in the window.

The window statistics are based only on records that are still retained in the tracker. `-dedup-ttl` directly affects what data the summary can see. To inspect a full 5-minute window, `-dedup-ttl` should be greater than or equal to `5m`, for example `-dedup-ttl 10m`. If the default `30s` is used, the 5-minute summary can only count records from roughly the most recent 30 seconds that have not yet been cleaned up.

`Summary(now, 0)` is the full summary, based on the accumulated `WinCount` and `DelaySamples` since the process started. The current HTTP `/summary` endpoint and automatic logs use the 5-minute window summary.

## Code flow

The main flow is in `main.go`:

1. Parse source, subscribe message, metrics, and TTL arguments.
2. Start one websocket goroutine for each source.
3. After each websocket connects successfully, send the corresponding `-source-subscribe` message.
4. After receiving a message, parse `messages[].blockHash`, `sequenceNumber`, and `blockNumber`.
5. `Tracker.RecordBlock` uses the block hash as the block ID. If there is no hash, it uses `block:<blockNumber>`.
6. The first source to see a block is recorded as the winner. Delays for later sources are calculated as `now - FirstSeenAt`.
7. When a block has been seen by all sources, mark it as completed, print the block details, and increment the winner's Prometheus counter by 1.
8. Records are stored in a min-heap by `FirstSeenAt` and are also indexed by block ID for fast lookup. Cleanup only pops expired records continuously from the heap head.
9. When generating a summary, copy a snapshot of the records and release the lock before calculation to avoid blocking websocket writes for too long.

Input messages are currently parsed according to the following JSON structure:

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

If a websocket message does not contain a recognizable block, it is skipped. When `-debug` is enabled, parse errors are printed.
