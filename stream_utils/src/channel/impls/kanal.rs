impl<T> crate::channel::sender::Sender for kanal::AsyncSender<T> {
    type Item = T;
    async fn send(&self, item: Self::Item) -> crate::channel::sender::Result {
        match kanal::AsyncSender::send(self, item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}

impl<T> crate::channel::receiver::Receiver for kanal::AsyncReceiver<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        kanal::AsyncReceiver::recv(self).await.ok()
    }
}
