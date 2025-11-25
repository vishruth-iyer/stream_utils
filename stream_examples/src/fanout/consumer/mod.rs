use stream_utils::fanout;

pub(crate) mod bufferer;
pub(crate) mod bytes_counter;

#[derive(bon::Builder, stream_utils::FanoutConsumerGroup, Clone)]
#[fanout_consumer_group_item_type(bytes::Bytes)]
#[fanout_consumer_group_output_derive(Debug)]
pub(super) struct DownloadFanoutConsumers<'a> {
    #[builder(into)]
    pub bytes_counter_1: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_2: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_3: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_4: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_5: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_6: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_7: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_8: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_9: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_10: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_11: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_12: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bytes_counter_13: fanout::consumer::ConsumerOrResolved<&'a bytes_counter::BytesCounter>,
    #[builder(into)]
    pub bufferer: fanout::consumer::ConsumerOrResolved<&'a bufferer::Bufferer>,
}

impl DownloadFanoutConsumersOutput<'_> {
    pub fn is_retryable(&self) -> bool {
        true
    }
    pub fn should_retry(&self) -> bool {
        self.bytes_counter_13.is_err()
    }
}
