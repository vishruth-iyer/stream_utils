pub trait Receiver {
    type Item;
    async fn recv(&mut self) -> Option<Self::Item>;
}

pub struct NoOpReceiver<T> {
    pub item: std::marker::PhantomData<T>,
}

impl<T> NoOpReceiver<T> {
    pub fn new() -> Self {
        Self {
            item: std::marker::PhantomData,
        }
    }
}

impl<T> Receiver for NoOpReceiver<T> {
    type Item = T;
    async fn recv(&mut self) -> Option<Self::Item> {
        None
    }
}

pub fn into_stream<Rx>(rx: Rx) -> impl futures::Stream<Item = Rx::Item>
where
    Rx: Receiver,
{
    futures::stream::unfold(rx, |mut rx| async {
        let item = rx.recv().await;
        item.map(|item| (item, rx))
    })
}
