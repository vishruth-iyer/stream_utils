use crate::channel::receiver::Receiver;

pub trait EgressItem {
    fn from_bytes(bytes: bytes::Bytes) -> Self;
    fn error() -> Self;
}

impl EgressItem for Result<bytes::Bytes, GenericError> {
    fn from_bytes(bytes: bytes::Bytes) -> Self {
        Self::Ok(bytes)
    }
    fn error() -> Self {
        Self::Err(GenericError)
    }
}

impl EgressItem for () {
    fn from_bytes(_bytes: bytes::Bytes) -> Self {
        ()
    }
    fn error() -> Self {
        ()
    }
}

pub trait EgressSender {
    type Item: EgressItem;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result;

    fn send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut crate::broadcaster::Broadcaster<BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: crate::channel::Channel<Item = bytes::Bytes>,
        BroadcasterChannel::Receiver: 'static,
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
        BroadcasterChannel: crate::channel::Channel<Item = bytes::Bytes>,
        BroadcasterChannel::Receiver: 'static;
}

impl<Tx> EgressSender for Tx
where
    Tx: crate::channel::sender::Sender,
    Tx::Item: EgressItem,
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
        BroadcasterChannel: crate::channel::Channel<Item = bytes::Bytes>,
        BroadcasterChannel::Receiver: 'static,
    {
        let mut rx = broadcaster.subscribe();
        let future = async move {
            while let Some(chunk) = rx.recv().await {
                self.send(Tx::Item::from_bytes(chunk)).await;
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
        BroadcasterChannel: crate::channel::Channel<Item = bytes::Bytes>,
        BroadcasterChannel::Receiver: 'static,
    {
        (broadcaster, futures::future::ready(()))
    }
}

#[derive(thiserror::Error, Debug)]
#[error("an error occurred")]
pub struct GenericError;
