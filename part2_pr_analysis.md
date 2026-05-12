# Part 2: Pull Request Analysis

## Repository: aio-libs/aiokafka

I selected **PR #193** and **PR #201** from the aiokafka repository. Both are consumer-side API additions that are self-contained and clearly scoped, making them easier to fully comprehend without needing deep knowledge of Kafka internals.

---

## PR #1: [#193 — Added `seek_to_beginning` and `seek_to_end` API](https://github.com/aio-libs/aiokafka/pull/193)

### PR Summary

Before this PR, the aiokafka consumer had no direct way to jump to the start or end of a partition's message log. If you wanted to replay all messages from scratch or skip straight to the latest messages, you had to manually figure out the offset numbers yourself and call the low-level `seek()` method. This PR adds two convenience methods — `seek_to_beginning()` and `seek_to_end()` — that handle that logic automatically. This matters in real use cases like replaying failed jobs (go to beginning) or fast-forwarding to live data (go to end) without making the developer calculate offsets manually. It brings the aiokafka consumer API closer to feature parity with the official Java Kafka consumer.

### Technical Changes

- **`aiokafka/consumer/consumer.py`** — Added the two new public methods `seek_to_beginning(*partitions)` and `seek_to_end(*partitions)` on the `AIOKafkaConsumer` class
- **`aiokafka/consumer/fetcher.py`** or subscription state — Internal changes to track which partitions need offset reset, likely adding entries to the offset reset map
- **`tests/test_consumer.py`** — New test cases verifying that calling these methods actually moves the consumer to offset 0 (beginning) or the latest offset (end) for the specified partitions
- **`CHANGES.rst`** — Changelog entry noting the new API addition

### Implementation Approach

The implementation works by registering the target partitions as needing an offset reset with a specific "strategy" — either `EARLIEST` for `seek_to_beginning` or `LATEST` for `seek_to_end`. This is how the underlying Kafka protocol works: instead of asking for a specific offset number, you ask the broker for the earliest or latest available offset for that partition, and the broker responds with the actual number.

When no partitions are passed as arguments, the methods apply to all currently assigned partitions. If specific partitions are provided, only those partitions are reset. The actual fetching of the real offset number from the broker is deferred until the next time the consumer tries to poll messages — this keeps the API non-blocking and consistent with aiokafka's async nature. Internally, the consumer maintains a dictionary of "pending offset resets" per partition, and before fetching messages, it resolves these pending resets by making an `OffsetRequest` to the broker.

### Potential Impact

This change is purely additive — it adds new public methods without modifying existing behavior. Existing code using `seek()` directly is unaffected. The main risk area is the offset reset logic inside the fetcher, which is a core part of the consumer loop. If the reset logic has a bug, it could cause the consumer to fetch from an unexpected position. The new methods become part of the public API surface, so any future changes to them would be breaking changes.

---

## PR #2: [#201 — Added `search_for_times` API to search offsets based on timestamps](https://github.com/aio-libs/aiokafka/pull/201)

### PR Summary

This PR adds a `offsets_for_times()` method (referred to as `search_for_times` in the PR title) to the aiokafka consumer. The problem it solves is time-based offset lookup: in Kafka, every message has a timestamp, and sometimes you want to resume consuming from "the point in time at 2pm yesterday" rather than knowing the exact offset number. Without this, developers would have to implement their own binary search or iterate through messages to find the right starting point — which is wasteful. This PR exposes Kafka's native ListOffsets API (added in Kafka 0.10.1) which lets you query the broker directly for "give me the first offset at or after this Unix timestamp." It's essential for applications that need time-based replay or recovery.

### Technical Changes

- **`aiokafka/consumer/consumer.py`** — Added `offsets_for_times(timestamps)` public method on `AIOKafkaConsumer`, taking a dict of `TopicPartition → timestamp_ms` and returning a dict of `TopicPartition → OffsetAndTimestamp`
- **`aiokafka/client.py`** or `aiokafka/cluster.py`** — Added support for sending `ListOffsetRequest` v1 (the timestamp-enabled version) to the appropriate broker for each partition
- **`aiokafka/structs.py`** — Likely added an `OffsetAndTimestamp` named tuple to hold the result (offset + the actual timestamp of that message)
- **`tests/test_consumer.py`** — Integration tests that produce messages with known timestamps and verify the returned offsets match expectations

### Implementation Approach

The implementation needs to route timestamp queries to the correct broker for each partition — this matters because in Kafka, each partition is "owned" by a specific broker (the leader), and offset requests must go to that leader. So the method first groups the requested partitions by their leader broker. Then it sends one `ListOffsetRequest v1` per broker (batching all that broker's partitions in a single request). Each request includes the timestamp in milliseconds for each partition. The broker responds with the earliest offset whose timestamp is greater than or equal to the requested time.

The results are collected from all brokers asynchronously and combined into a single dict returned to the caller. Special sentinel values handle edge cases: if no message exists after the given timestamp (the timestamp is in the future or beyond the retention period), the broker returns a null result for that partition.

### Potential Impact

This touches the cluster metadata and client request-routing logic, which is a more central part of the library. Any bug in how partitions are mapped to brokers could result in sending requests to the wrong node and getting incorrect results. The feature also requires Kafka broker version 0.10.1 or higher — using it against older Kafka clusters would fail at runtime. This API is especially critical for stream processing applications that need point-in-time recovery after outages.
