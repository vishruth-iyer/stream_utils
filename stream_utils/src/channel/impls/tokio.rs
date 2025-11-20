impl<T> crate::channel::receiver::Receiver for tokio::sync::mpsc::Receiver<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        self.recv().await
    }
}

impl<T> crate::channel::sender::Sender for tokio::sync::mpsc::Sender<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match self.send(item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}
