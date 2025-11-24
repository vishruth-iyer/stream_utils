use stream_utils::fanout;

#[derive(Clone, Copy)]
pub(crate) struct BytesCounter {
    limit: usize,
}

impl BytesCounter {
    pub(crate) fn new() -> Self {
        Self::new_with_limit(usize::MAX)
    }
    pub(crate) fn new_with_limit(limit: usize) -> Self {
        Self { limit }
    }
}

impl fanout::consumer::FanoutConsumer for BytesCounter {
    type Item = bytes::Bytes;
    type Output = usize;
    type Error = crate::fanout::Error;
    async fn consume_from_fanout<Rx>(
        &self,
        mut rx: Rx,
        cancellation_token: stream_utils::broadcaster::CancellationToken,
        _content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: stream_utils::channel::receiver::Receiver<Item = bytes::Bytes>,
    {
        let mut i = 0;
        while let Some(bytes) = rx.recv().await {
            i += bytes.len();
            if i > self.limit {
                cancellation_token.cancel();
                return Err(crate::fanout::Error("too big".to_string()));
            }
        }
        Ok(i)
    }
}
