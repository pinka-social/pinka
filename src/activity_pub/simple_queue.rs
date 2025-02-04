//! A simple persited queue

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use fjall::{Keyspace, Partition, PersistMode, UserKey};
use minicbor::{Decode, Encode};
use tracing::debug;

type Bytes = [u8; 16];

#[derive(Debug, Encode, Decode)]
pub(super) struct QueueMessage {
    #[n(0)]
    pub(super) body: Vec<u8>,
    #[n(1)]
    pub(super) receipt_handle: Bytes,
    #[n(2)]
    pub(super) approximate_receive_count: u64,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct ReceiveResult {
    #[n(0)]
    pub(super) key: Bytes,
    #[n(1)]
    pub(super) message: QueueMessage,
}

impl ReceiveResult {
    pub(super) fn to_bytes(&self) -> Result<Vec<u8>> {
        minicbor::to_vec(self).context("unable to encode ReceiveResult")
    }
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<ReceiveResult> {
        minicbor::decode(bytes).context("unable to decode ReceiveResult")
    }
}

#[derive(Clone)]
pub(super) struct SimpleQueue {
    keyspace: Keyspace,
    messages: Partition,
    visibility: Partition,
}

impl SimpleQueue {
    pub(super) fn new(keyspace: Keyspace) -> Result<SimpleQueue> {
        let messages = keyspace.open_partition("sq_messages", Default::default())?;
        let visibility = keyspace.open_partition("sq_visibility", Default::default())?;
        Ok(SimpleQueue {
            keyspace,
            messages,
            visibility,
        })
    }

    pub(super) fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after unix epoch")
            .as_secs()
    }
    pub(super) fn is_empty(&self) -> Result<bool> {
        self.messages
            .is_empty()
            .context("unable to read from messages tree")
    }
    pub(super) fn send_message(&self, queue_name: &str, key: Bytes, body: &[u8]) -> Result<()> {
        let receipt_handle = key;

        let message = QueueMessage {
            body: body.into(),
            receipt_handle,
            approximate_receive_count: 0,
        };

        debug!(target: "sq", queue_name, ?key, ?message, "send message");

        let q_key = q_key(queue_name, key);
        let bytes = minicbor::to_vec(message)?;

        self.messages.insert(q_key, bytes)?;
        self.keyspace.persist(PersistMode::SyncAll)?;

        Ok(())
    }
    // Use client side generated `now` to track visibility timeout so it is
    // consistent across system restart or crashes. Each instance might have
    // slightly different time drift, but messages will eventually become
    // visible.
    pub(super) fn receive_message(
        &self,
        queue_name: &str,
        new_receipt_handle: Bytes,
        now: u64,
        visibility_timeout: u64,
    ) -> Result<Option<ReceiveResult>> {
        for item in self.messages.prefix(queue_name) {
            let (key, value_bytes) = item?;

            // Check visibility
            if let Some(visible_at) = self.visibility.get(&key)? {
                let visible_at = u64::from_le_bytes(visible_at.as_ref().try_into()?);
                if visible_at > now {
                    continue;
                }
            }

            let mut message: QueueMessage = minicbor::decode(&value_bytes)?;
            let new_visible_at = now + visibility_timeout;

            // Update in atomic batch
            let mut batch = self.keyspace.batch();
            batch.insert(&self.visibility, key.clone(), new_visible_at.to_le_bytes());

            message.receipt_handle = new_receipt_handle;
            message.approximate_receive_count += 1;
            let bytes = minicbor::to_vec(&message)?;
            batch.insert(&self.messages, key.clone(), bytes);

            batch.commit()?;
            self.keyspace.persist(PersistMode::SyncAll)?;

            let key = key
                .strip_prefix(queue_name.as_bytes())
                .expect("key should be prefixed with the queue name");

            debug!(target: "sq", queue_name, ?key, ?message, "received message");

            return Ok(Some(ReceiveResult {
                key: key.as_ref().try_into()?,
                message,
            }));
        }

        Ok(None)
    }
    pub(super) fn delete_message(
        &self,
        queue_name: &str,
        key: Bytes,
        receipt_handle: Bytes,
    ) -> Result<bool> {
        let q_key = q_key(queue_name, key);
        let mut batch = self.keyspace.batch();

        if let Some(message) = self.messages.get(&q_key)? {
            let message: QueueMessage = minicbor::decode(&message)?;
            if message.receipt_handle == receipt_handle {
                debug!(target: "sq", queue_name, ?key, ?message, "delete message");
                batch.remove(&self.messages, q_key.clone());
                batch.remove(&self.visibility, q_key.clone());
            } else {
                return Ok(false);
            }
        }

        batch.commit()?;
        self.keyspace.persist(PersistMode::SyncAll)?;
        Ok(true)
    }
}

fn q_key(queue_name: &str, key: [u8; 16]) -> UserKey {
    let mut q_key = vec![];
    q_key.extend_from_slice(queue_name.as_bytes());
    q_key.extend_from_slice(&key);
    q_key.into()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;
    use uuid::{Bytes, Uuid};

    use super::{ReceiveResult, SimpleQueue};

    const QUEUE_NAME: &str = "test_queue";

    fn uuidgen() -> Bytes {
        Uuid::now_v7().into_bytes()
    }

    #[test]
    fn test_basic_flow() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        // Test empty queue
        assert!(queue
            .receive_message(QUEUE_NAME, uuidgen(), 1, 30)?
            .is_none());

        // Send message
        queue.send_message(QUEUE_NAME, uuidgen(), b"test1")?;

        // Receive message
        let handle = uuidgen();
        let ReceiveResult { key, message: msg } =
            queue.receive_message(QUEUE_NAME, handle, 2, 30)?.unwrap();
        assert_eq!(msg.body, b"test1");
        assert_eq!(handle, msg.receipt_handle);

        // Delete message
        queue.delete_message(QUEUE_NAME, key, handle)?;

        // Verify deletion
        assert!(queue
            .receive_message(QUEUE_NAME, uuidgen(), 3, 30)?
            .is_none());
        Ok(())
    }

    #[test]
    fn test_visibility_timeout() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        queue.send_message(QUEUE_NAME, uuidgen(), b"test2")?;

        // First receive
        let handle1 = uuidgen();
        let ReceiveResult {
            key: key1,
            message: msg1,
        } = queue.receive_message(QUEUE_NAME, handle1, 1, 1)?.unwrap();

        // Immediate retry should find nothing
        assert!(queue
            .receive_message(QUEUE_NAME, uuidgen(), 1, 1)?
            .is_none());

        // Wait longer than timeout
        let handle2 = uuidgen();
        let ReceiveResult {
            key: key2,
            message: msg2,
        } = queue.receive_message(QUEUE_NAME, handle2, 10, 1)?.unwrap();
        assert_eq!(key1, key2);
        assert_eq!(msg1.body, msg2.body);
        assert_ne!(handle1, handle2);

        Ok(())
    }

    #[test]
    fn test_handle_rotation() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        queue.send_message(QUEUE_NAME, uuidgen(), b"test3")?;

        let handle1 = uuidgen();
        let ReceiveResult {
            key: key1,
            message: _,
        } = queue.receive_message(QUEUE_NAME, handle1, 1, 0)?.unwrap();
        let handle2 = uuidgen();
        let ReceiveResult {
            key: key2,
            message: _,
        } = queue.receive_message(QUEUE_NAME, handle2, 2, 0)?.unwrap();

        assert_eq!(key1, key2);
        assert_ne!(handle1, handle2);

        assert!(!queue.delete_message(QUEUE_NAME, key1, handle1)?);
        assert!(queue.delete_message(QUEUE_NAME, key2, handle2)?);

        Ok(())
    }

    #[test]
    fn test_concurrent_access() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        let mut handles = vec![];

        for i in 0..10 {
            let q = queue.clone();
            handles.push(std::thread::spawn(move || {
                q.send_message(QUEUE_NAME, uuidgen(), format!("msg{i}").as_bytes())
                    .unwrap();
            }));
        }

        for _ in 0..5 {
            let q = queue.clone();
            handles.push(std::thread::spawn(move || {
                while let Some(ReceiveResult { key, message }) = q
                    .receive_message(QUEUE_NAME, uuidgen(), SimpleQueue::now(), 30)
                    .unwrap()
                {
                    q.delete_message(QUEUE_NAME, key, message.receipt_handle)
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(queue
            .receive_message(QUEUE_NAME, uuidgen(), SimpleQueue::now(), 30)?
            .is_none());
        Ok(())
    }

    #[test]
    fn test_concurrent_multi_queue() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        let mut handles = vec![];
        let message_count = 5;

        // Concurrent producers for different queues
        for queue_name in ["queue_a", "queue_b"] {
            for i in 0..message_count {
                let q = queue.clone();
                let q_name = queue_name.to_string();
                handles.push(std::thread::spawn(move || {
                    q.send_message(&q_name, uuidgen(), format!("{q_name}_msg{i}").as_bytes())
                        .unwrap();
                }));
            }
        }

        // Concurrent consumers for different queues
        for queue_name in ["queue_a", "queue_b"] {
            for _ in 0..2 {
                // 2 consumers per queue
                let q = queue.clone();
                let q_name = queue_name.to_string();
                handles.push(std::thread::spawn(move || {
                    while let Some(ReceiveResult { key, message }) = q
                        .receive_message(&q_name, uuidgen(), SimpleQueue::now(), 30)
                        .unwrap()
                    {
                        // Verify message belongs to the correct queue
                        assert!(message.body.starts_with(q_name.as_bytes()));
                        q.delete_message(&q_name, key, message.receipt_handle)
                            .unwrap();
                    }
                }));
            }
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify both queues are empty
        assert!(queue
            .receive_message("queue_a", uuidgen(), SimpleQueue::now(), 1)?
            .is_none());
        assert!(queue
            .receive_message("queue_b", uuidgen(), SimpleQueue::now(), 1)?
            .is_none());

        Ok(())
    }
}
