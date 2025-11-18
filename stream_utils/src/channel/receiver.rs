pub trait Receiver<T> {
    async fn recv(&mut self) -> Option<T>;
}

impl<T> Receiver<T> for () {
    async fn recv(&mut self) -> Option<T> {
        None
    }
}

pub fn into_stream<T>(rx: impl Receiver<T>) -> impl futures::Stream<Item = T> {
    futures::stream::unfold(rx, |mut rx| async {
        let item = rx.recv().await;
        item.map(|item| (item, rx))
    })
}
