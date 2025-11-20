use crate::{broadcaster, channel::receiver::Receiver};

pub trait Sender {
    type Item;
    async fn send(&self, item: Self::Item) -> Result;

    fn send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: super::Channel,
        Self::Item: From<BroadcasterChannel::Item>,
        BroadcasterChannel::Receiver: 'static,
    {
        self._send_from_broadcaster(broadcaster).1
    }
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: super::Channel,
        Self::Item: From<BroadcasterChannel::Item>,
        BroadcasterChannel::Receiver: 'static,
    {
        let mut rx = broadcaster.subscribe();
        let future = async move {
            while let Some(chunk) = rx.recv().await {
                self.send(Self::Item::from(chunk)).await;
            }
        };
        (broadcaster, future)
    }
}

pub struct NoOpSender<T> {
    item: std::marker::PhantomData<T>,
}

impl<T> NoOpSender<T> {
    pub fn new() -> Self {
        Self {
            item: std::marker::PhantomData,
        }
    }
}

impl<T> Sender for NoOpSender<T> {
    type Item = T;
    async fn send(&self, _item: Self::Item) -> Result {
        Result::Failure
    }
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: super::Channel,
        Self::Item: From<BroadcasterChannel::Item>,
        BroadcasterChannel::Receiver: 'static, {
        (broadcaster, futures::future::ready(()))
    }
}

pub enum Result {
    Success,
    Failure,
}
