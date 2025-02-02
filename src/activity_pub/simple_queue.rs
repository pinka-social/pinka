//! A simple persited queue

use std::time::Instant;

use anyhow::{Context, Result};
use fjall::{Keyspace, Partition, PersistMode};
use minicbor::{Decode, Encode};
use uuid::{Bytes, Uuid};

#[derive(Debug, Encode, Decode)]
pub(super) struct QueueMessage {
    #[n(0)]
    body: Vec<u8>,
    #[n(1)]
    receipt_handle: Bytes,
}

#[derive(Debug, Encode, Decode)]
pub(super) struct ReceiveResult {
    #[n(0)]
    key: Bytes,
    #[n(1)]
    message: QueueMessage,
    #[n(2)]
    receipt_handle: Bytes,
}

impl ReceiveResult {
    pub(super) fn to_bytes(&self) -> Result<Vec<u8>> {
        minicbor::to_vec(self).context("unable to encode ReceiveResult")
    }
    pub(super) fn from_bytes(bytes: &[u8]) -> Result<ReceiveResult> {
        minicbor::decode(bytes).context("unable to encode ReceiveResult")
    }
}

#[derive(Clone)]
pub(super) struct SimpleQueue {
    keyspace: Keyspace,
    messages: Partition,
    visibility: Partition,
    epoch: Instant,
}

impl SimpleQueue {
    pub(super) fn new(keyspace: Keyspace) -> Result<SimpleQueue> {
        let messages = keyspace.open_partition("sq_messages", Default::default())?;
        let visibility = keyspace.open_partition("sq_visibility", Default::default())?;
        let epoch = Instant::now();
        Ok(SimpleQueue {
            keyspace,
            messages,
            visibility,
            epoch,
        })
    }
    pub(super) fn send_message(&self, body: &[u8]) -> Result<()> {
        let uuid = Uuid::now_v7();
        let receipt_handle = Uuid::now_v7();

        let message = QueueMessage {
            body: body.into(),
            receipt_handle: receipt_handle.into_bytes(),
        };

        let bytes = minicbor::to_vec(message)?;

        self.messages.insert(uuid.as_bytes(), bytes)?;
        self.keyspace.persist(PersistMode::SyncAll)?;

        Ok(())
    }
    pub(super) fn receive_message(&self, visibility_timeout: u64) -> Result<Option<ReceiveResult>> {
        let now = self.epoch.elapsed().as_secs();

        for item in self.messages.iter() {
            let (key_bytes, value_bytes) = item?;
            let key = Uuid::from_bytes(key_bytes.as_ref().try_into()?);

            // Check visibility
            if let Some(visible_at) = self.visibility.get(&key)? {
                let visible_at = u64::from_le_bytes(visible_at.as_ref().try_into()?);
                if visible_at > now {
                    continue;
                }
            }

            let mut message: QueueMessage = minicbor::decode(&value_bytes)?;
            let new_receipt_handle = Uuid::now_v7();
            let new_visible_at = now + visibility_timeout;

            // Update in atomic batch
            let mut batch = self.keyspace.batch();
            batch.insert(
                &self.visibility,
                key.into_bytes(),
                new_visible_at.to_le_bytes(),
            );

            message.receipt_handle = new_receipt_handle.into_bytes();
            let bytes = minicbor::to_vec(&message)?;
            batch.insert(&self.messages, key.into_bytes(), bytes);

            batch.commit()?;
            self.keyspace.persist(PersistMode::SyncAll)?;

            return Ok(Some(ReceiveResult {
                key: key.into_bytes(),
                message,
                receipt_handle: new_receipt_handle.into_bytes(),
            }));
        }

        Ok(None)
    }
    pub(super) fn delete_message(&self, key: Bytes, receipt_handle: Bytes) -> Result<bool> {
        let mut batch = self.keyspace.batch();

        if let Some(message) = self.messages.get(&key)? {
            let message: QueueMessage = minicbor::decode(&message)?;
            if message.receipt_handle == receipt_handle {
                batch.remove(&self.messages, key.clone());
                batch.remove(&self.visibility, key.clone());
            } else {
                return Ok(false);
            }
        }

        batch.commit()?;
        self.keyspace.persist(PersistMode::SyncAll)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::{ReceiveResult, SimpleQueue};

    #[test]
    fn test_basic_flow() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        // Test empty queue
        assert!(queue.receive_message(30)?.is_none());

        // Send message
        queue.send_message(b"test1")?;

        // Receive message
        let ReceiveResult {
            key,
            message: msg,
            receipt_handle: handle,
        } = queue.receive_message(30)?.unwrap();
        assert_eq!(msg.body, b"test1");
        assert_eq!(handle, msg.receipt_handle);

        // Delete message
        queue.delete_message(key, handle)?;

        // Verify deletion
        assert!(queue.receive_message(30)?.is_none());
        Ok(())
    }

    #[test]
    fn test_visibility_timeout() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        queue.send_message(b"test2")?;

        // First receive
        let ReceiveResult {
            key: key1,
            message: msg1,
            receipt_handle: handle1,
        } = queue.receive_message(1)?.unwrap();

        // Immediate retry should find nothing
        assert!(queue.receive_message(1)?.is_none());

        // Wait longer than timeout
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Should receive again with new handle
        let ReceiveResult {
            key: key2,
            message: msg2,
            receipt_handle: handle2,
        } = queue.receive_message(1)?.unwrap();
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

        queue.send_message(b"test3")?;

        let ReceiveResult {
            key: key1,
            message: _,
            receipt_handle: handle1,
        } = queue.receive_message(0)?.unwrap();
        let ReceiveResult {
            key: key2,
            message: _,
            receipt_handle: handle2,
        } = queue.receive_message(0)?.unwrap();

        assert_eq!(key1, key2);
        assert_ne!(handle1, handle2);

        // Old handle should fail deletion
        assert!(!queue.delete_message(key1, handle1)?);

        // New handle should work
        assert!(queue.delete_message(key2, handle2)?);

        Ok(())
    }

    #[test]
    fn test_concurrent_access() -> Result<()> {
        let dir = tempdir()?;
        let keyspace = fjall::Config::new(dir.path()).temporary(true).open()?;
        let queue = SimpleQueue::new(keyspace)?;

        let mut handles = vec![];

        // Spawn producers
        for i in 0..10 {
            let q = queue.clone();
            handles.push(std::thread::spawn(move || {
                q.send_message(format!("msg{i}").as_bytes()).unwrap();
            }));
        }

        // Spawn consumers
        for _ in 0..5 {
            let q = queue.clone();
            handles.push(std::thread::spawn(move || {
                while let Some(ReceiveResult {
                    key,
                    message: _,
                    receipt_handle: handle,
                }) = q.receive_message(30).unwrap()
                {
                    q.delete_message(key, handle).unwrap();
                }
            }));
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all processed
        assert!(queue.receive_message(30)?.is_none());
        Ok(())
    }
}
