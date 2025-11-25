use crate::{broadcaster, channel};

pub trait FanoutConsumer {
    type Item;
    type Output;
    type Error;
    async fn consume_from_fanout<Rx>(
        &self,
        rx: Rx,
        cancellation_token: broadcaster::CancellationToken,
        content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: channel::receiver::Receiver<Item = Self::Item>;
}

impl<'a, Consumer> FanoutConsumer for &'a Consumer
where
    Consumer: FanoutConsumer
{
    type Item = Consumer::Item;
    type Output = Consumer::Output;
    type Error = Consumer::Error;
    async fn consume_from_fanout<Rx>(
        &self,
        rx: Rx,
        cancellation_token: broadcaster::CancellationToken,
        content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: channel::receiver::Receiver<Item = Self::Item>,
    {
        (*self).consume_from_fanout(rx, cancellation_token, content_length).await
    }
}

pub trait FanoutConsumerGroup {
    type Item;
    type Output: CancelEgress;
    fn consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        fanout_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> impl Future<Output = Self::Output> + 'a
    where
        Channel: channel::Channel<Item = Self::Item>,
        Channel::Receiver: 'static,
    {
        self._consume_from_fanout(fanout_broadcaster, content_length)
            .1
    }
    // the above is the only signature I want, but we need the below to tell the compiler that the future doesn't borrow the broadcaster
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        fanout_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Self::Output> + 'a,
    )
    where
        Channel: channel::Channel<Item = Self::Item>,
        Channel::Receiver: 'static;
    fn retry(self, previous_output: &Self::Output) -> Self;
}

#[derive(Clone, Copy, Debug)]
pub enum ConsumerOrResolved<Consumer: FanoutConsumer> {
    Consumer(Consumer),
    ConsumerOutput(Consumer::Output),
}

impl<Consumer> From<Consumer> for ConsumerOrResolved<Consumer>
where
    Consumer: FanoutConsumer,
{
    fn from(value: Consumer) -> Self {
        Self::Consumer(value)
    }
}

impl<Consumer> FanoutConsumerGroup for ConsumerOrResolved<Consumer>
where
    Consumer: FanoutConsumer,
    Consumer::Output: Clone,
{
    type Item = Consumer::Item;
    type Output = Result<Consumer::Output, Consumer::Error>;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        fanout_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Self::Output> + 'a,
    )
    where
        Channel: channel::Channel<Item = Self::Item>,
        Channel::Receiver: 'static,
    {
        let future = match self {
            Self::Consumer(consumer) => {
                tokio_util::either::Either::Left(consumer.consume_from_fanout(
                    fanout_broadcaster.subscribe(),
                    fanout_broadcaster.get_cancellation_token().clone(),
                    content_length,
                ))
            }
            Self::ConsumerOutput(consumer_output) => tokio_util::either::Either::Right(
                futures::future::ready(Ok(consumer_output.clone())),
            ),
        };
        (fanout_broadcaster, future)
    }
    fn retry(self, previous_output: &Self::Output) -> Self {
        match previous_output {
            Ok(output) => Self::ConsumerOutput(output.clone()),
            Err(_) => match self {
                Self::Consumer(consumer) => Self::Consumer(consumer),
                Self::ConsumerOutput(output) => Self::ConsumerOutput(output.clone()),
            },
        }
    }
}

impl<Consumer> FanoutConsumerGroup for Consumer
where
    Consumer: FanoutConsumer,
{
    type Item = Consumer::Item;
    type Output = Result<Consumer::Output, Consumer::Error>;
    fn _consume_from_fanout<'a, 'b, Channel>(
        &'a self,
        fanout_broadcaster: &'b mut broadcaster::Broadcaster<Channel>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Channel>,
        impl Future<Output = Self::Output> + 'a,
    )
    where
        Channel: channel::Channel<Item = Self::Item>,
        Channel::Receiver: 'static,
    {
        let future = self.consume_from_fanout(fanout_broadcaster.subscribe(), fanout_broadcaster.get_cancellation_token().clone(), content_length);
        (fanout_broadcaster, future)
    }
    fn retry(self, _previous_output: &Self::Output) -> Self {
        self
    }
}

pub trait CancelEgress {
    fn cancel_egress(&self) -> bool;
}

impl<T, E> CancelEgress for Result<T, E> {
    fn cancel_egress(&self) -> bool {
        self.is_err()
    }
}
