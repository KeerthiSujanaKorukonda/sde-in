# Part 4: Technical Communication

## Task 4.1: Scenario Response

**Reviewer's question:** "Why did you choose this specific PR over the others? What made it comprehensible to you, and what challenges do you anticipate in implementing it?"

---

I chose PR #201 (`offsets_for_times`) over the others primarily because the problem it solves is something I've encountered practically — needing to resume message consumption from a specific point in time rather than a raw offset number. That real-world context made it much easier to reason about what "correct" looks like, which is critical for writing good acceptance criteria and edge cases.

From a technical standpoint, the PR is scoped at the right level for me. The consumer API layer in aiokafka is Python I can read and understand: it's async function calls, dict manipulation, and structured data — not low-level protocol bit-packing or Cython extension code. I'm comfortable with `asyncio`, `async/await`, and the pattern of fanning out concurrent requests with `asyncio.gather`, which is the heart of this implementation.

I avoided some of the other PRs deliberately. PR #196 (separate socket groups) touches the connection management internals — managing multiple socket connections per broker node is a systems-level problem involving TCP lifecycle, connection state machines, and thread safety considerations that would be harder for me to fully specify correctly. PRs in the airbyte repo (#11317 etc.) involve Java/Kotlin platform code mixed with Python connectors, and since the codebase is multi-language, the blast radius of getting something wrong is wider.

The main implementation challenge I anticipate is the broker routing logic — specifically, making sure partition-to-broker-leader mapping is done correctly using the cluster metadata, and handling the case where metadata is stale (the leader has changed since the last metadata refresh). A second challenge is the protocol layer: sending `ListOffsetRequest v1` requires knowing the exact wire format, and getting byte offsets wrong in the request builder would cause silent failures or confusing errors.

To overcome these, I'd start by reading how existing offset fetch requests are sent in `aiokafka/client.py` and `aiokafka/protocol/offset.py` to understand the pattern before writing new code. For broker routing, I'd trace how `seek()` currently resolves partition leaders and follow the same path. For testing, I'd rely heavily on the existing integration test setup with the Docker Kafka container, since unit tests alone won't catch protocol-level bugs.
