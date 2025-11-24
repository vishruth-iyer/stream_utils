use stream_utils::fanout;

#[derive(Debug)]
pub(crate) struct Bufferer;

impl fanout::consumer::FanoutConsumer for Bufferer {
    type Item = bytes::Bytes;
    type Output = Vec<bytes::Bytes>;
    type Error = std::convert::Infallible;
    async fn consume_from_fanout<Rx>(
        &self,
        mut rx: Rx,
        _cancellation_token: stream_utils::broadcaster::CancellationToken,
        content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: stream_utils::channel::receiver::Receiver<Item = bytes::Bytes>,
    {
        let mut buffer = Vec::with_capacity(
            content_length
                .map(|content_length| content_length / 8192)
                .unwrap_or_default() as usize,
        );
        while let Some(chunk) = rx.recv().await {
            buffer.push(chunk);
        }
        Ok(buffer)
    }
}
