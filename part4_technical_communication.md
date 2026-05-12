# Part 4: Technical Communication

## Task 4.1: Scenario Response

Reviewer's question: "Why did you choose this specific PR over the others? What made it comprehensible to you, and what challenges do you anticipate in implementing it?"

---

I chose PR #201, the offsets_for_times implementation, over the others primarily because the problem it solves is something I have encountered practically. Needing to resume message consumption from a specific point in time rather than a raw offset number is a real, common pain point in data pipeline work. That real-world context made it much easier to reason about what correct behavior looks like, which is critical when writing acceptance criteria and edge cases.

From a technical standpoint, the PR is scoped at the right level for me. The consumer API layer in aiokafka is Python I can read and understand clearly. It involves async function calls, dictionary manipulation, and structured data types rather than low-level protocol bit-packing or Cython extension code. I am comfortable with asyncio and the pattern of fanning out concurrent requests with asyncio.gather, which is the heart of this implementation.

I avoided some of the other PRs deliberately. PR #196, which adds separate socket groups per node, touches the connection management internals at a systems level. Managing multiple socket connections per broker involves TCP lifecycle handling, connection state machines, and thread safety considerations that would be harder for me to fully specify correctly without deeper Kafka networking knowledge. PRs in the airbyte repository involve Java and Kotlin platform code mixed with Python connectors, and since the codebase is multi-language, the blast radius of getting something wrong is wider and harder to test confidently.

The main implementation challenge I anticipate is the broker routing logic, specifically making sure the partition-to-broker-leader mapping is done correctly using cluster metadata, and handling the case where that metadata is stale because the leader has changed since the last refresh. A second challenge is the protocol layer. Sending ListOffsetRequest version 1 requires knowing the exact wire format, and getting any field wrong in the request builder would cause silent failures or confusing errors at the broker side.

To overcome these, I would start by reading how existing offset fetch requests are sent in aiokafka/client.py and aiokafka/protocol/offset.py to understand the established pattern before writing any new code. For broker routing, I would trace how seek() currently resolves partition leaders and follow the same approach. For testing, I would rely heavily on the existing integration test setup with the Docker Kafka container since unit tests alone will not catch protocol-level bugs where the request is structurally wrong.
