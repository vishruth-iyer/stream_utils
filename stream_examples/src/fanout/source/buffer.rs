use stream_utils::fanout;

pub(crate) struct BytesSource(Vec<bytes::Bytes>);

impl From<Vec<bytes::Bytes>> for BytesSource {
    fn from(value: Vec<bytes::Bytes>) -> Self {
        Self(value)
    }
}

impl fanout::source::FanoutSource for BytesSource {
    type Item = bytes::Bytes;
    type Error = super::super::Error;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        Ok(Some(
            self.0.iter().map(|bytes| bytes.len()).sum::<usize>() as u64
        ))
    }
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: stream_utils::broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: stream_utils::channel::Channel<Item = bytes::Bytes>,
    {
        for chunk in &self.0 {
            let _ = broadcaster.broadcast(chunk.clone()).await;
        }
        Ok(())
    }
    fn reset(self) -> Option<Self> {
        Some(self)
    }
}
