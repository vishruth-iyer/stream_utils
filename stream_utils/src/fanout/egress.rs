use crate::channel::receiver::Receiver;

pub trait EgressItem<BroadcastItem> {
    fn from_broadcast_item(item: BroadcastItem) -> Self;
    fn error() -> Self;
}

#[derive(thiserror::Error, Debug)]
#[error("an error occurred")]
pub struct GenericError;

impl<BroadcastItem> EgressItem<BroadcastItem> for Result<BroadcastItem, GenericError> {
    fn from_broadcast_item(item: BroadcastItem) -> Self {
        Self::Ok(item)
    }
    fn error() -> Self {
        Self::Err(GenericError)
    }
}

impl<T> EgressItem<T> for () {
    fn from_broadcast_item(_item: T) -> Self {
        ()
    }
    fn error() -> Self {
        ()
    }
}

pub trait EgressSender {
    type Item;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result;

    fn send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: crate::channel::Channel,
        BroadcasterChannel::Receiver: 'static,
        Self::Item: EgressItem<BroadcasterChannel::Item>,
    {
        self._send_from_broadcaster(broadcaster).1
    }
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> (
        &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: crate::channel::Channel,
        BroadcasterChannel::Receiver: 'static,
        Self::Item: EgressItem<BroadcasterChannel::Item>;
}

impl<Tx> EgressSender for Tx
where
    Tx: crate::channel::sender::Sender,
{
    type Item = Tx::Item;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        crate::channel::sender::Sender::send(self, item).await
    }
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> (
        &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: crate::channel::Channel,
        BroadcasterChannel::Receiver: 'static,
        Self::Item: EgressItem<BroadcasterChannel::Item>,
    {
        let mut rx = broadcaster.subscribe();
        let future = async move {
            while let Some(chunk) = rx.recv().await {
                self.send(Self::Item::from_broadcast_item(chunk)).await;
            }
        };
        (broadcaster, future)
    }
}

impl EgressSender for () {
    type Item = ();
    async fn send(&self, _item: Self::Item) -> crate::channel::sender::Result {
        crate::channel::sender::Result::Failure
    }

    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> (
        &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: crate::channel::Channel,
        BroadcasterChannel::Receiver: 'static,
    {
        (broadcaster, futures::future::ready(()))
    }
}
