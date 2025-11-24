use crate::{broadcaster, channel};

pub trait FanoutSource: Send + Sync + 'static + Sized {
    type Item: Clone;
    type Error: super::MaybeRetryable;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error>;
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: channel::Channel<Item = Self::Item>;
    fn reset(self) -> Option<Self>;
}

impl<S, T, E> FanoutSource for S
where
    S: futures::Stream<Item = Result<T, E>> + Unpin + Send + Sync + 'static + Sized,
    T: Clone,
    E: super::MaybeRetryable,
{
    type Item = T;
    type Error = E;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        Ok(None)
    }
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: channel::Channel<Item = T>,
    {
        broadcaster.broadcast_from_result_stream(self).await
    }
    fn reset(self) -> Option<Self> {
        None
    }
}
