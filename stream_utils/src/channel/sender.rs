use crate::{broadcaster, channel::receiver::Receiver};

pub trait Sender<T> {
    async fn send(&self, item: T) -> Result;

    fn send_from_broadcaster<'a, 'b, U, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<U, BroadcasterChannel>,
    ) -> impl Future<Output = ()> + 'a
    where
        BroadcasterChannel: super::Channel<U>,
        T: From<U>,
        BroadcasterChannel::Receiver: 'static,
    {
        self._send_from_broadcaster(broadcaster).1
    }
    fn _send_from_broadcaster<'a, 'b, U, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<U, BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<U, BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: super::Channel<U>,
        T: From<U>,
        BroadcasterChannel::Receiver: 'static,
    {
        let mut rx = broadcaster.subscribe();
        let future = async move {
            while let Some(chunk) = rx.recv().await {
                self.send(T::from(chunk)).await;
            }
        };
        (broadcaster, future)
    }
}

impl<T> Sender<T> for () {
    async fn send(&self, _item: T) -> Result {
        Result::Failure
    }

    fn _send_from_broadcaster<'a, 'b, U, BroadcasterChannel>(
        &'a self,
        broadcaster: &'b mut broadcaster::Broadcaster<U, BroadcasterChannel>,
    ) -> (
        &'b mut broadcaster::Broadcaster<U, BroadcasterChannel>,
        impl Future<Output = ()> + 'a,
    )
    where
        BroadcasterChannel: super::Channel<U>,
        T: From<U>,
        BroadcasterChannel::Receiver: 'static,
    {
        (broadcaster, futures::future::ready(()))
    }
}

pub enum Result {
    Success,
    Failure,
}
