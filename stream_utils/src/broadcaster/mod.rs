use futures::{Stream, StreamExt, future::join_all};

use crate::channel::{self, sender::Sender};

#[derive(Default, Debug, Clone)]
pub struct CancellationToken(tokio_util::sync::CancellationToken);

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

/// A collection of channels.
#[derive(bon::Builder, Debug)]
pub struct Broadcaster<Channel: channel::Channel> {
    channel: Channel,
    buffer_size: usize,
    #[builder(default)]
    senders: Vec<Channel::Sender>,
    #[builder(default)]
    cancellation_token: CancellationToken,
}

impl<Channel> Broadcaster<Channel>
where
    Channel: channel::Channel,
{
    pub fn subscribe(&mut self) -> Channel::Receiver {
        let (tx, rx) = self.channel.create_channel(self.buffer_size);
        self.senders.push(tx.into());
        rx.into()
    }

    pub fn get_cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }
}

impl<Channel> Broadcaster<Channel>
where
    Channel: channel::Channel,
    Channel::Item: Clone,
{
    pub async fn broadcast(&self, item: Channel::Item) -> Result<(), BroadcastError> {
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

    pub async fn broadcast_and_prune(&mut self, item: Channel::Item) -> Result<(), BroadcastError> {
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
                channel::sender::Result::Success => Some(tx),
                channel::sender::Result::Failure => None,
            })
            .collect();

        Ok(())
    }

    pub async fn broadcast_from_stream(
        &self,
        mut stream: impl Stream<Item = Channel::Item> + Unpin,
    ) {
        while let Some(item) = stream.next().await {
            if let Err(BroadcastError::ReceiverFailure) = self.broadcast(item).await {
                break;
            }
        }
    }

    pub async fn broadcast_from_result_stream<E>(
        &self,
        mut stream: impl Stream<Item = Result<Channel::Item, E>> + Unpin,
    ) -> Result<(), E> {
        while let Some(item) = stream.next().await.transpose()? {
            if let Err(BroadcastError::ReceiverFailure) = self.broadcast(item).await {
                return Ok(());
            }
        }
        Ok(())
    }

    // private helper
    async fn broadcast_item(&self, item: Channel::Item) -> Vec<channel::sender::Result> {
        // send messages concurrently
        join_all(self.senders.iter().map(|tx| {
            let item = item.clone();
            tx.send(item)
        }))
        .await
    }
}

#[cfg(test)]
mod tests;
