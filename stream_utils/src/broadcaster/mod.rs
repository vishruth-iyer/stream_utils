use futures::{Stream, StreamExt, future::join_all};

pub mod channel;

static BROADCASTER_DEFAULT_BUFFER_SIZE: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| {
        std::env::var("BROADCASTER_DEFAULT_BUFFER_SIZE")
            .ok()
            .and_then(|var| var.parse::<usize>().ok())
            .unwrap_or(1)
    });

#[derive(Debug, Clone)]
pub struct CancellationToken(tokio_util::sync::CancellationToken);

impl CancellationToken {
    fn new() -> Self {
        Self(tokio_util::sync::CancellationToken::new())
    }
}

impl From<tokio_util::sync::CancellationToken> for CancellationToken {
    fn from(value: tokio_util::sync::CancellationToken) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for CancellationToken {
    type Target = tokio_util::sync::CancellationToken;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub enum BroadcastError {
    ReceiverFailure,
}

/// A collection of `mpsc` channels.
///
/// I explored other `mpmc`/`broadcast` channels but they all had issues with our use case:
/// - most `mpmc` channels only have a single consumer pick up each message, here we want all consumers to pick up all messages
/// - `tokio::sync::broadcast` has no backpressure on the sender, so if consumers are slow old messages will be dropped. here we want to guarantee that every consumer receives every message
#[derive(Debug)]
pub struct Broadcaster<T> {
    senders: Vec<channel::Sender<T>>,
    buffer_size: usize,
    cancellation_token: CancellationToken,
}

impl<T> Broadcaster<T> {
    pub fn new() -> Self {
        Self::new_with_buffer_size(*BROADCASTER_DEFAULT_BUFFER_SIZE)
    }

    pub fn new_with_buffer_size(buffer_size: usize) -> Self {
        Self {
            senders: Vec::new(),
            buffer_size,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn subscribe(&mut self) -> channel::Receiver<T> {
        let (tx, rx) = channel::new(self.buffer_size);
        self.senders.push(tx.into());
        rx.into()
    }

    pub fn get_cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }
}

impl<T: Clone> Broadcaster<T> {
    pub async fn broadcast(&self, item: T) -> Result<(), BroadcastError> {
        tokio::select! {
            biased; // no need for random polling; always poll cancellation token first then broadcast
            _ = self.cancellation_token.cancelled() => {
                Err(BroadcastError::ReceiverFailure)
            }
            _ = self.broadcast_item(item) => {
                Ok(())
            }
        }
    }

    pub async fn broadcast_and_prune(&mut self, item: T) -> Result<(), BroadcastError> {
        let send_results = tokio::select! {
            biased; // no need for random polling; always poll cancellation token first then broadcast
            _ = self.cancellation_token.cancelled() => {
                Err(BroadcastError::ReceiverFailure)
            }
            send_results = self.broadcast_item(item) => {
                Ok(send_results)
            }
        }?;

        // remove senders whose receivers are dead
        self.senders = self
            .senders
            .drain(..)
            .zip(send_results)
            .filter_map(|(tx, send_result)| match send_result {
                channel::SendResult::Success => Some(tx),
                channel::SendResult::Failure => None,
            })
            .collect();

        Ok(())
    }

    pub async fn broadcast_from_stream<E>(
        &self,
        mut stream: impl Stream<Item = Result<T, E>> + Unpin,
    ) -> Result<(), E> {
        while let Some(item) = stream.next().await.transpose()? {
            if let Err(BroadcastError::ReceiverFailure) = self.broadcast(item).await {
                return Ok(());
            }
        }
        Ok(())
    }
}

// private helpers

impl<T: Clone> Broadcaster<T> {
    async fn broadcast_item(&self, item: T) -> Vec<channel::SendResult> {
        // send messages concurrently
        join_all(self.senders.iter().map(|tx| {
            let item = item.clone();
            tx.send(item)
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::Broadcaster;

    #[tokio::test]
    async fn test_broadcaster_all_receivers_receive_all_messages_in_order() {
        let messages = (0..5).collect_vec();
        let mut broadcaster = Broadcaster::new_with_buffer_size(1);
        let mut rx1 = broadcaster.subscribe();
        let mut rx2 = broadcaster.subscribe();
        let broadcast_future = async {
            for message in messages.clone() {
                broadcaster.broadcast(message).await.unwrap();
            }
            drop(broadcaster)
        };
        let rx1_future = async {
            let mut received_messages = Vec::new();
            let expected_messages = messages.clone();
            while let Some(message) = rx1.recv().await {
                received_messages.push(message);
            }
            drop(rx1);
            assert_eq!(
                received_messages.len(),
                expected_messages.len(),
                "received messages: {received_messages:?}, expected: {expected_messages:?}"
            );
            for (received_message, expected_message) in
                received_messages.into_iter().zip(expected_messages)
            {
                assert_eq!(
                    received_message, expected_message,
                    "received message: {received_message}, expected: {expected_message}"
                );
            }
        };
        let rx2_future = async {
            let mut received_messages = Vec::new();
            let expected_messages = messages.clone();
            while let Some(message) = rx2.recv().await {
                received_messages.push(message);
            }
            drop(rx2);
            assert_eq!(
                received_messages.len(),
                expected_messages.len(),
                "received messages: {received_messages:?}, expected: {expected_messages:?}"
            );
            for (received_message, expected_message) in
                received_messages.into_iter().zip(expected_messages)
            {
                assert_eq!(
                    received_message, expected_message,
                    "received message: {received_message}, expected: {expected_message}"
                );
            }
        };
        tokio::join!(broadcast_future, rx1_future, rx2_future);
    }

    /// Check that the broadcaster aborts when it receives the cancellation token signal
    ///
    /// 1. Broadcaster sends message 0.
    /// 2. Both receivers receive message 0.
    /// 3. Broadcaster sends message 1.
    /// 4. Both receivers receive message 1.
    ///    * One receiver signals via the cancellation token to abort the broadcast
    /// 5. Now there are two possibilities (race condition):
    ///    * Either the broadcaster receives the signal before it broadcasts message 2, in which case it fails to broadcast message 2
    ///    * Or the broadcaster receives the signal after it broadcasts message 2, in which case it succeeds in broadcasting message 2 but fails to broadcast message 3
    ///
    /// The test ensures that the broadcaster fails to broadcast either message 2 or message 3 and no receiver receives a message >= 3
    #[tokio::test]
    async fn test_broadcaster_abort_if_cancellation_token() {
        let messages = (0..5).collect_vec();
        let mut broadcaster = Broadcaster::new_with_buffer_size(1);
        let cancellation_token = broadcaster.get_cancellation_token().clone();
        let mut rx1 = broadcaster.subscribe();
        let mut rx2 = broadcaster.subscribe();
        let broadcast_future = async {
            for message in messages.clone() {
                let broadcast_result = broadcaster.broadcast(message).await;
                if broadcast_result.is_err() {
                    assert!(message == 2 || message == 3, "message: {message}");
                    break;
                }
            }
            drop(broadcaster);
        };
        let rx1_future = async {
            while let Some(message) = rx1.recv().await {
                if message == 1 {
                    cancellation_token.cancel();
                    break;
                }
            }
            drop(rx1);
        };
        let rx2_future = async {
            while let Some(message) = rx2.recv().await {
                assert!(message < 3, "message: {message}");
            }
            drop(rx2);
        };
        tokio::join!(broadcast_future, rx1_future, rx2_future);
    }
}
