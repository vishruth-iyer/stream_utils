use bytes::Bytes;

use crate::{broadcaster, channel};

pub trait FanoutSource: Send + Sync + 'static + Sized {
    type Error;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error>;
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: channel::Channel<Item = Bytes>;
    fn reset(self) -> Option<Self>;
}
