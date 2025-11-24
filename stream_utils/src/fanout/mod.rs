use crate::{
    broadcaster, channel,
    fanout::source::FanoutSource,
};

pub mod consumer;
pub mod driver;
pub mod egress;
pub mod source;

pub struct Ready;
pub struct Used;

pub type ReadyStreamFanout<Source, Consumers> = StreamFanout<Source, Consumers, Ready>;
pub type UsedStreamFanout<Source, Consumers> = StreamFanout<Source, Consumers, Used>;

pub struct StreamFanout<Source, Consumers, State> {
    source: Source,
    consumers: Consumers,
    state: std::marker::PhantomData<State>,
}

impl<Source, Consumers, State> StreamFanout<Source, Consumers, State> {
    pub fn into_parts(self) -> (Source, Consumers) {
        (self.source, self.consumers)
    }
}

impl<Source, Consumers> ReadyStreamFanout<Source, Consumers> {
    pub fn new(source: Source, consumers: Consumers) -> Self {
        Self {
            source,
            consumers,
            state: std::marker::PhantomData,
        }
    }

    pub fn into_driver(self) -> driver::StreamFanoutDriver<Source, Consumers, (), (), ()> {
        driver::StreamFanoutDriver::new(self)
    }
}

impl<Source, Consumers> ReadyStreamFanout<Source, Consumers>
where
    Source: FanoutSource,
    Consumers: consumer::FanoutConsumerGroup<Item = Source::Item>,
{
    #[cfg_attr(feature = "tracing", tracing::instrument(name = "stream_fanout", skip_all))]
    async fn drive_inner<BroadcasterChannel, EgressItem, EgressSender>(
        &mut self,
        broadcaster_channel: BroadcasterChannel,
        broadcaster_buffer_size: usize,
        egress_tx: &EgressSender,
    ) -> Result<Consumers::Output, Source::Error>
    where
        BroadcasterChannel: channel::Channel<Item = Source::Item>,
        BroadcasterChannel::Receiver: 'static,
        EgressItem: egress::EgressItem<BroadcasterChannel::Item>,
        EgressSender: egress::EgressSender<Item = EgressItem>,
    {
        #[cfg(feature = "tracing")]
        let start = tokio::time::Instant::now();

        let content_length = self.source.get_content_length().await?;

        let mut fanout_broadcaster = broadcaster::Broadcaster::builder()
            .channel(broadcaster_channel)
            .buffer_size(broadcaster_buffer_size)
            .build();

        // create subscriber futures
        let consumers_future = self
            .consumers
            .consume_from_fanout(&mut fanout_broadcaster, content_length);
        let egress_future = egress_tx.send_from_broadcaster(&mut fanout_broadcaster);

        // broadcast from stream
        let stream_broadcast_future = self.source.broadcast(fanout_broadcaster);

        // poll futures concurrently
        let (stream_broadcast_result, consumers_output, _) =
            tokio::join!(stream_broadcast_future, consumers_future, egress_future);

        #[cfg(feature = "tracing")]
        tracing::info!(
            response_time = start.elapsed().as_millis(),
            "done sending items to subscribers from fanout stream",
        );

        // propagate errors
        stream_broadcast_result?;

        Ok(consumers_output)
    }

    fn into_used(self) -> UsedStreamFanout<Source, Consumers> {
        StreamFanout { source: self.source, consumers: self.consumers, state: std::marker::PhantomData }
    }
}

pub trait MaybeRetryable {
    fn is_retryable(&self) -> bool;
    fn should_retry(&self) -> bool;
}

impl<T, E> MaybeRetryable for Result<T, E>
where
    E: MaybeRetryable,
{
    fn is_retryable(&self) -> bool {
        self.is_ok() || self.as_ref().is_err_and(E::is_retryable)
    }
    fn should_retry(&self) -> bool {
        self.is_err()
    }
}

impl MaybeRetryable for std::convert::Infallible {
    fn is_retryable(&self) -> bool {
        true
    }
    fn should_retry(&self) -> bool {
        false
    }
}
