use stream_utils::download_fanout;

#[derive(Clone, Copy)]
pub(super) struct BytesCounter {
    limit: usize,
}

impl BytesCounter {
    pub(super) fn new() -> Self {
        Self::new_with_limit(usize::MAX)
    }
    pub(super) fn new_with_limit(limit: usize) -> Self {
        Self { limit }
    }
}

impl download_fanout::consumer::FanoutConsumer for BytesCounter {
    type Output = usize;
    type Error = super::Error;
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
                return Err(super::Error("too big".to_string()));
            }
        }
        Ok(i)
    }
}

#[derive(bon::Builder, stream_utils::FanoutConsumerGroup, Clone)]
#[fanout_consumer_group_error_ty(super::Error)]
#[fanout_consumer_group_output_derive(Debug)]
pub(super) struct DownloadFanoutConsumers {
    #[builder(into)]
    pub bytes_counter_1: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_2: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_3: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_4: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_5: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_6: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_7: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_8: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_9: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_10: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_11: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_12: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    pub bytes_counter_13: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
}
