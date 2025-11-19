impl<T> crate::channel::receiver::Receiver<T> for kanal::AsyncReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        (&*self).recv().await.ok()
    }
}

impl<T> crate::channel::sender::Sender<T> for kanal::AsyncSender<T> {
    async fn send(&self, item: T) -> crate::channel::sender::Result {
        match self.send(item).await {
            Ok(_) => crate::channel::sender::Result::Success,
            Err(_) => crate::channel::sender::Result::Failure,
        }
    }
}
