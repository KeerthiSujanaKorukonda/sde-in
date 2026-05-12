# Part 3: Prompt Preparation

## Chosen PR: #201 - offsets_for_times / search_for_times API

Link: https://github.com/aio-libs/aiokafka/pull/201

---

## 3.1.1 Repository Context

aiokafka is a Python library that lets developers write async Kafka producers and consumers using Python's asyncio framework. Kafka itself is a distributed message broker. Think of it as a high-speed, ordered log of messages organized into topics, where each topic is split into partitions that live on different broker servers.

The problem aiokafka solves is that the official Python Kafka client called kafka-python is synchronous, meaning every network call blocks the calling thread until the broker responds. In modern Python web apps and data pipelines built on asyncio, such as those using aiohttp or FastAPI, blocking calls are a serious problem because they freeze the entire event loop. aiokafka wraps the Kafka protocol with async/await so that all network operations yield control back to the event loop while waiting, allowing thousands of concurrent operations in a single thread.

The primary users are backend Python developers building real-time data pipelines, event-driven microservices, or stream processing systems where they cannot afford to use synchronous I/O. For example, a service that reads user activity events from Kafka and writes analytics to a database would use aiokafka's consumer to fetch messages without blocking other concurrent operations.

The repository has two main components. AIOKafkaProducer is for sending messages and AIOKafkaConsumer is for reading them. Internally there is a lower-level AIOKafkaClient that manages TCP connections to the broker cluster, handles protocol serialization and deserialization, and deals with partition leader lookups via cluster metadata. The consumer layer sits on top of this client. The codebase also uses Cython extensions for performance-critical protocol encoding and decoding paths.

---

## 3.1.2 Pull Request Description

This PR adds a new method called offsets_for_times() to AIOKafkaConsumer. The method accepts a dictionary mapping TopicPartition objects to Unix timestamps in milliseconds and returns a dictionary mapping each TopicPartition to an OffsetAndTimestamp result. That result contains the first message offset whose timestamp is at or after the requested time, plus that message's actual timestamp.

Before this PR, if a developer wanted to resume consuming from everything after 3pm yesterday, they had two bad options. Either scan through messages manually until passing the target time, which is slow and wasteful, or maintain their own external offset-to-timestamp index, which is complex and fragile. Neither approach works well in production.

The new behavior works like this: you call await consumer.offsets_for_times() with a dict of partitions and timestamps and get back the exact offset to seek to before starting to consume. The previous behavior simply did not have this method, so trying to call it would give an AttributeError. The new behavior makes async network calls to Kafka's ListOffsets API version 1, which supports timestamp-based lookup, and returns structured results.

The change requires Kafka broker version 0.10.1 or higher since that is when timestamp-based offset lookup was added to the Kafka protocol. The PR also introduces the OffsetAndTimestamp named tuple as a new return type to hold both the offset number and the actual timestamp of the found message.

---

## 3.1.3 Acceptance Criteria

When offsets_for_times() is called with a valid dict of TopicPartition to timestamp_ms, the system should return a dict of TopicPartition to OffsetAndTimestamp where the offset points to the first message at or after the given timestamp.

When multiple partitions are queried in a single call, including partitions on different brokers, the system should batch requests per broker and return correct results for all partitions in a single awaited response.

When a timestamp is provided that is beyond the latest available message, for example a future timestamp, the system should return None for that partition's value rather than raising an exception, matching the behavior of the official Java Kafka client.

When a timestamp of -1 representing latest or -2 representing earliest is passed, the implementation should handle these special sentinel values correctly per the Kafka ListOffsets protocol specification.

When the method is called against a Kafka broker older than version 0.10.1, which does not support ListOffsetRequest v1, the system should raise an informative error rather than returning garbage data or silently failing.

The implementation should not block the asyncio event loop. The method must be a proper coroutine defined with async def, and all broker communication must use await.

The returned OffsetAndTimestamp type should expose both an offset attribute and a timestamp attribute so callers can verify the actual message time at the returned offset.

---

## 3.1.4 Edge Cases

**Edge Case 1: Timestamp beyond the retention window**

If you request an offset for a timestamp older than the broker's log retention period, for example the topic only keeps 7 days of data but you ask for 30 days ago, all those messages have been deleted. The broker will return the earliest available offset, not None. The implementation must clearly document this behavior so callers do not assume they are getting messages from exactly that time.

**Edge Case 2: Partitions split across multiple brokers with one broker down**

In a Kafka cluster, different partitions are leaders on different broker nodes. If the requested partitions span three brokers and broker number two is unreachable, the method needs to decide how to handle the failure. It should not silently return only the partitions that succeeded while dropping the rest. The caller needs to know which partitions failed and why.

**Edge Case 3: Empty topic partitions with no messages ever written**

If a partition has never had any messages written to it, there is no first offset at or after timestamp X because the log is empty. The broker may return offset 0 or a null result. The code must handle this without throwing an unhandled exception, especially since the caller may be querying a newly created topic.

**Edge Case 4: Requesting offsets for partitions not assigned to this consumer**

The consumer may only be assigned certain partitions. If the caller passes in a TopicPartition that the consumer does not know about or is not currently part of the cluster metadata, the metadata lookup for the broker leader will fail. This should surface as a clear error message, not a cryptic KeyError buried in internal code.

---

## 3.1.5 Initial Prompt

You are working on aiokafka, an asyncio-based Python client for Apache Kafka. The repository is at https://github.com/aio-libs/aiokafka. The codebase targets Python 3.6 and above and uses asyncio coroutines throughout. All network operations must use async def and await for broker communication. No blocking calls are allowed anywhere in the implementation.

Your task is to implement the offsets_for_times(timestamps) method on the AIOKafkaConsumer class as described in PR #201.

What the method does:

The method accepts a dictionary of TopicPartition to timestamp_ms where timestamp_ms is a Unix timestamp in milliseconds as an integer. It queries the Kafka brokers using the ListOffsets request version 1 to find, for each requested partition, the earliest message offset whose timestamp is greater than or equal to the given timestamp. It returns a dictionary of TopicPartition to OffsetAndTimestamp or None.

Files you will likely need to modify:

- aiokafka/consumer/consumer.py - add the public async def offsets_for_times(self, timestamps) method
- aiokafka/structs.py - add an OffsetAndTimestamp named tuple with fields offset and timestamp
- aiokafka/client.py - add support for sending ListOffsetRequest version 1 routed to the correct partition leader broker
- tests/test_consumer.py - add integration tests

Implementation steps to follow:

Step 1 - Add OffsetAndTimestamp as a namedtuple with fields offset and timestamp to structs.py.

Step 2 - In consumer.py, implement offsets_for_times() as an async def method that first validates all input dict keys are TopicPartition instances, then groups partitions by their leader broker using cluster metadata via self._client, then sends one ListOffsetRequest version 1 per broker with all that broker's partitions and their timestamps, then awaits all broker responses concurrently using asyncio.gather, then assembles the results back into a TopicPartition to OffsetAndTimestamp dict, and finally returns None as the value for any partition where the broker returned a null or empty result.

Acceptance criteria to satisfy:

Multiple partitions across different brokers must work correctly in a single call. Future timestamps must return None and must not raise an exception. The method must be a proper coroutine with no blocking calls anywhere. Partitions with no messages should return None without crashing. If a partition's broker is unavailable, raise a KafkaTimeoutError with a clear descriptive message.

Edge cases to handle:

If the timestamp is in the future or beyond the retention period, return None for that partition. If the partition has no messages ever written, return None and do not crash. If the partition is not found in cluster metadata, raise UnknownTopicOrPartitionError. If the broker version is too old to support ListOffsetRequest version 1, raise a KafkaError with a version-related message.

Testing requirements:

Write at least three integration tests. The first test is a basic happy path where you produce messages with known timestamps, call offsets_for_times, and verify the returned offset is correct. The second test uses a future timestamp and verifies that None is returned. The third test queries three partitions simultaneously and verifies all results are correct and complete.

Use the existing test patterns in tests/test_consumer.py. They use pytest-asyncio with async def test_ style functions and a running Docker Kafka instance. Follow those patterns exactly so the new tests integrate cleanly with the existing test suite.

---

### Declaration

I declare that all written content in this assessment is my own work, created without the use of AI language models or automated writing tools. All technical analysis and documentation reflects my personal understanding and has been written in my own words.
