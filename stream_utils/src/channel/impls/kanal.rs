impl<T> crate::channel::receiver::Receiver for kanal::AsyncReceiver<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        (&*self).recv().await.ok()
    }
}

impl<T> crate::channel::sender::Sender for kanal::AsyncSender<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match self.send(item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}
