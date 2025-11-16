use crate::{
    broadcaster,
    channel::{self, receiver::Receiver, sender::Sender},
};

pub type Item = Result<bytes::Bytes, GenericError>;

pub trait Channel: channel::Channel<Item> {
    fn send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        tx: &'a Self::Sender,
        broadcaster: &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: channel::Channel<bytes::Bytes>,
        BroadcasterChannel::Receiver: 'a;
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        tx: &'a Self::Sender,
        broadcaster: &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: channel::Channel<bytes::Bytes>,
        BroadcasterChannel::Receiver: 'a;
}

impl<T> Channel for T
where
    T: channel::Channel<Item>,
{
    fn send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        tx: &'a Self::Sender,
        broadcaster: &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: channel::Channel<bytes::Bytes>,
        BroadcasterChannel::Receiver: 'a,
    {
        Self::_send_from_broadcaster(tx, broadcaster).1
    }
    fn _send_from_broadcaster<'a, 'b, BroadcasterChannel>(
        tx: &'a Self::Sender,
        broadcaster: &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<bytes::Bytes, BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: channel::Channel<bytes::Bytes>,
        BroadcasterChannel::Receiver: 'a,
    {
        let mut rx = broadcaster.subscribe();
        let future = async move {
            while let Some(chunk) = rx.recv().await {
                tx.send(Ok(chunk)).await;
            }
        };
        (broadcaster, future)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("an error occurred")]
pub struct GenericError;
