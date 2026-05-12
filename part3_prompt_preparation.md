# Part 3: Prompt Preparation

## Chosen PR: [#201 — `offsets_for_times` / `search_for_times` API](https://github.com/aio-libs/aiokafka/pull/201)

---

## 3.1.1 Repository Context

aiokafka is a Python library that lets developers write async Kafka producers and consumers using Python's `asyncio` framework. Kafka itself is a distributed message broker — think of it as a high-speed, ordered log of messages, organized into "topics," where each topic is split into "partitions" that live on different broker servers.

The problem aiokafka solves is that the official Python Kafka client (`kafka-python`) is synchronous — meaning every network call blocks the calling thread until the broker responds. In modern Python web apps and data pipelines built on `asyncio` (like those using `aiohttp` or `FastAPI`), blocking calls are a serious problem because they freeze the entire event loop. aiokafka wraps the Kafka protocol with `async/await` so that all network operations yield control back to the event loop while waiting, allowing thousands of concurrent operations in a single thread.

The primary users are backend Python developers building real-time data pipelines, event-driven microservices, or stream processing systems where they cannot afford to use synchronous I/O. For example, a service that reads user activity events from Kafka and writes analytics to a database would use aiokafka's consumer to fetch messages without blocking other concurrent operations.

The repository has two main components: `AIOKafkaProducer` for sending messages and `AIOKafkaConsumer` for reading them. Internally there is a lower-level `AIOKafkaClient` that manages TCP connections to the broker cluster, handles protocol serialization/deserialization, and deals with partition leader lookups via cluster metadata. The consumer layer sits on top of this client. The codebase uses Cython extensions for performance-critical protocol encoding/decoding paths.

---

## 3.1.2 Pull Request Description

This PR adds a new method called `offsets_for_times()` to `AIOKafkaConsumer`. The method accepts a dictionary mapping `TopicPartition` objects to Unix timestamps (in milliseconds) and returns a dictionary mapping each `TopicPartition` to an `OffsetAndTimestamp` result — meaning the first message offset whose timestamp is at or after the requested time, plus that message's actual timestamp.

Before this PR, if a developer wanted to resume consuming from "everything after 3pm yesterday," they had two bad options: either scan through messages manually until they passed the target time (slow and wasteful), or maintain their own external offset-to-timestamp index (complex and fragile). Neither is acceptable in production.

The new behavior works like this: you call `await consumer.offsets_for_times({TopicPartition("my-topic", 0): 1609459200000})` and get back the exact offset to seek to before you start consuming. Previous behavior: no such method existed — you would get an `AttributeError` if you tried to call it. New behavior: the method makes async network calls to Kafka's `ListOffsets` API (version 1, which supports timestamp-based lookup) and returns structured results.

The change requires Kafka broker version 0.10.1 or higher, since that's when timestamp-based offset lookup was added to the Kafka protocol. The PR also likely introduces the `OffsetAndTimestamp` named tuple as a new return type to hold both the offset number and the actual timestamp of the found message.

---

## 3.1.3 Acceptance Criteria

✓ When `offsets_for_times()` is called with a valid `{TopicPartition: timestamp_ms}` dict, the system should return a dict of `{TopicPartition: OffsetAndTimestamp}` where the offset points to the first message at or after the given timestamp.

✓ When multiple partitions are queried in a single call (including partitions on different brokers), the system should batch requests per broker and return correct results for all partitions in a single awaited response.

✓ When a timestamp is provided that is beyond the latest available message (e.g., a future timestamp), the system should return `None` for that partition's value rather than raising an exception, matching the behavior of the official Java Kafka client.

✓ When a timestamp of `-1` (representing "latest") or `-2` (representing "earliest") is passed, the implementation should handle these special sentinel values correctly per the Kafka ListOffsets protocol spec.

✓ When the method is called against a Kafka broker older than version 0.10.1 (which does not support `ListOffsetRequest v1`), the system should raise an informative error rather than returning garbage data or silently failing.

✓ The implementation should not block the `asyncio` event loop — the method must be a proper coroutine (`async def`) and all broker communication must use `await`.

✓ The returned `OffsetAndTimestamp` type should expose both `.offset` and `.timestamp` attributes so callers can verify the actual message time at the returned offset.

---

## 3.1.4 Edge Cases

**Edge Case 1: Timestamp beyond retention window**
If you request an offset for a timestamp that is older than the broker's log retention period (e.g., the topic only keeps 7 days of data but you ask for 30 days ago), all those messages have been deleted. The broker will return the earliest available offset, not `None`. The implementation must clearly document this behavior so callers don't assume they're getting messages from exactly that time.

**Edge Case 2: Partitions split across multiple brokers with one broker down**
In a Kafka cluster, different partitions are leaders on different broker nodes. If the requested partitions span 3 brokers and broker #2 is unreachable, the method needs to decide: fail the whole request, return partial results, or retry with a different replica. The implementation should not silently return only the partitions that succeeded while dropping the rest.

**Edge Case 3: Empty topic partitions (no messages ever written)**
If a partition has never had any messages written to it, there is no "first offset at or after timestamp X" — the log is empty. The broker may return offset 0 or a null result. The code must handle this without throwing an unhandled exception, especially since the caller may be querying a newly created topic.

**Edge Case 4: Requesting offsets for partitions not assigned to this consumer**
The consumer may only be assigned certain partitions. If the caller passes in a `TopicPartition` that the consumer doesn't know about or isn't currently part of the cluster metadata, the metadata lookup for the broker leader will fail. This should return a clear error, not a cryptic `KeyError` buried in internal code.

---

## 3.1.5 Initial Prompt

You are working on **aiokafka**, an `asyncio`-based Python client for Apache Kafka. The repository is at `https://github.com/aio-libs/aiokafka`. The codebase is Python 3.6+ and uses `asyncio` coroutines throughout — all network operations must be `async def` and use `await` for broker communication.

**Your task:** Implement the `offsets_for_times(timestamps)` method on the `AIOKafkaConsumer` class, as described in PR #201.

**What the method does:**
The method accepts a dictionary of `{TopicPartition: timestamp_ms}` where `timestamp_ms` is a Unix timestamp in milliseconds (integer). It queries the Kafka brokers using the `ListOffsets` request (version 1) to find, for each requested partition, the earliest message offset whose timestamp is greater than or equal to the given timestamp. It returns a dictionary of `{TopicPartition: OffsetAndTimestamp | None}`.

**Files you will likely need to modify:**
- `aiokafka/consumer/consumer.py` — add the public `async def offsets_for_times(self, timestamps)` method
- `aiokafka/structs.py` — add an `OffsetAndTimestamp` named tuple with fields `offset` and `timestamp`
- `aiokafka/client.py` — add support for sending `ListOffsetRequest v1` routed to the correct partition leader broker
- `tests/test_consumer.py` — add integration tests

**Implementation steps to follow:**
1. Add `OffsetAndTimestamp = namedtuple('OffsetAndTimestamp', ['offset', 'timestamp'])` to `structs.py`
2. In `consumer.py`, implement `offsets_for_times()` as an `async def` method that:
   a. Validates the input dict keys are all `TopicPartition` instances
   b. Groups partitions by their leader broker (use cluster metadata via `self._client`)
   c. Sends one `ListOffsetRequest v1` per broker with all that broker's partitions and their timestamps
   d. Awaits all broker responses concurrently (use `asyncio.gather`)
   e. Assembles the results back into a `{TopicPartition: OffsetAndTimestamp}` dict
   f. Returns `None` as the value for any partition where the broker returned a null/empty result

**Acceptance criteria to satisfy:**
- Multiple partitions across different brokers must work in one call
- Future timestamps must return `None`, not raise an exception
- The method must be a proper coroutine — no blocking calls
- Partitions with no messages should return `None` without crashing
- If a partition's broker is unavailable, raise a `KafkaTimeoutError` with a clear message

**Edge cases to handle:**
- Timestamp is in the future or beyond retention: return `None` for that partition
- Empty partitions with no messages ever written: return `None`, do not crash
- Partition not found in cluster metadata: raise `UnknownTopicOrPartitionError`
- Broker version too old to support `ListOffsetRequest v1`: raise `KafkaError` with a version message

**Testing requirements:**
Write at least 3 integration tests:
1. Basic happy path: produce messages with known timestamps, call `offsets_for_times`, verify the returned offset is correct
2. Future timestamp: call with a timestamp 1 year in the future, verify `None` is returned
3. Multi-partition: query 3 partitions simultaneously and verify all results are correct

Use the existing test patterns in `tests/test_consumer.py` — they use `pytest-asyncio` with an `async def test_*` style and a running Docker Kafka instance.
