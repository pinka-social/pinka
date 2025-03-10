# Scheduled feed ingestion

We use a simple queue with a static key as a distributed lock, the visibility
timeout as lock timeout.

Whenever a new state machine is started, it should enqueue one item with the
static key. The feed slurp should periodically fetch this item. The visibility
timeout ensures only one feed slurp is executing in the ideal condition.

When the task fails or it took too long, the next feed slurp check will see the
released lock.