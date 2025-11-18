impl<T: Unpin + Send + 'static> crate::channel::sender::Sender<T> for crossfire::AsyncTx<T> {
    async fn send(&self, item: T) -> crate::channel::sender::Result {
        match self.send(item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}

impl<T: Unpin + Send + 'static> crate::channel::sender::Sender<T> for crossfire::MAsyncTx<T> {
    async fn send(&self, item: T) -> crate::channel::sender::Result {
        match crossfire::AsyncTxTrait::send(self, item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}
