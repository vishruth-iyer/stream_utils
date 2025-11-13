use bytes::Bytes;

use crate::broadcaster;

pub trait FanoutSource: Send + Sync + 'static + Sized {
    type Error: std::fmt::Display;
    fn get_original_url(&self) -> Option<&str>;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error>;
    async fn broadcast(
        &mut self,
        broadcaster: broadcaster::Broadcaster<Bytes>,
    ) -> Result<(), Self::Error>;
    fn reset(self) -> Option<Self>;
}
