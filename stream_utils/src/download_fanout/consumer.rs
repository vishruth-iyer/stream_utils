use bytes::Bytes;

use crate::{broadcaster, channel};

pub trait FanoutConsumer {
    type Output;
    type Error;
    async fn consume_from_fanout<Rx>(
        &self,
        rx: Rx,
        cancellation_token: broadcaster::CancellationToken,
        content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: channel::receiver::Receiver<Item = Bytes>;
}

#[derive(Clone, Copy, Debug)]
pub enum ConsumerOrResolved<Consumer: FanoutConsumer> {
    Consumer(Consumer),
    ConsumerOutput(Consumer::Output),
}

impl<Consumer: FanoutConsumer> From<Consumer> for ConsumerOrResolved<Consumer> {
    fn from(value: Consumer) -> Self {
        Self::Consumer(value)
    }
}

pub trait FanoutConsumerGroup {
    type Output;
    type Error;
    fn consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + 'a
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static,
    {
        self._consume_from_fanout(download_broadcaster, content_length)
            .1
    }
    // the above is the only signature I want, but we need the below to tell the compiler that the future doesn't borrow the broadcaster
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    )
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static;
}

impl<ConsumerGroup: FanoutConsumerGroup> FanoutConsumerGroup for std::sync::Arc<ConsumerGroup> {
    type Output = ConsumerGroup::Output;
    type Error = ConsumerGroup::Error;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    )
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static,
    {
        self.as_ref()
            ._consume_from_fanout(download_broadcaster, content_length)
    }
}

impl<ConsumerGroup: FanoutConsumerGroup> FanoutConsumerGroup for std::rc::Rc<ConsumerGroup> {
    type Output = ConsumerGroup::Output;
    type Error = ConsumerGroup::Error;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    )
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static,
    {
        self.as_ref()
            ._consume_from_fanout(download_broadcaster, content_length)
    }
}

impl<'consumer_group, ConsumerGroup: FanoutConsumerGroup> FanoutConsumerGroup
    for &'consumer_group ConsumerGroup
{
    type Output = ConsumerGroup::Output;
    type Error = ConsumerGroup::Error;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    )
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static,
    {
        (*self)._consume_from_fanout(download_broadcaster, content_length)
    }
}

impl<Consumer> FanoutConsumerGroup for ConsumerOrResolved<Consumer>
where
    Consumer: FanoutConsumer,
    Consumer::Output: Clone,
{
    type Output = Consumer::Output;
    type Error = Consumer::Error;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    )
    where
        Channel: channel::Channel<Item = Bytes>,
        Channel::Receiver: 'static,
    {
        let future = match self {
            Self::Consumer(consumer) => {
                tokio_util::either::Either::Left(consumer.consume_from_fanout(
                    download_broadcaster.subscribe(),
                    download_broadcaster.get_cancellation_token().clone(),
                    content_length,
                ))
            }
            Self::ConsumerOutput(consumer_output) => tokio_util::either::Either::Right(
                futures::future::ready(Ok(consumer_output.clone())),
            ),
        };
        (download_broadcaster, future)
    }
}
