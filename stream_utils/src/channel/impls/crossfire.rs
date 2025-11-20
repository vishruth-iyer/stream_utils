impl<T: Unpin + Send + 'static> crate::channel::sender::Sender for crossfire::AsyncTx<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match self.send(item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
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
