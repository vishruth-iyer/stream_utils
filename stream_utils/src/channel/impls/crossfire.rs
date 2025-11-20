impl<T: Unpin + Send + 'static> crate::channel::sender::Sender for crossfire::AsyncTx<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match crossfire::AsyncTx::send(self, item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}

impl<T> crate::channel::receiver::Receiver for crossfire::AsyncRx<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        crossfire::AsyncRx::recv(self).await.ok()
    }
}

impl<T: Unpin + Send + 'static> crate::channel::sender::Sender for crossfire::MAsyncTx<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match crossfire::AsyncTxTrait::send(self, item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}

impl<T: Unpin + Send + 'static> crate::channel::receiver::Receiver for crossfire::MAsyncRx<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        crossfire::AsyncRxTrait::recv(self).await.ok()
    }
}
