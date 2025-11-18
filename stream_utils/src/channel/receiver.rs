pub trait Receiver<T> {
    async fn recv(&mut self) -> Option<T>;
}

impl<T> Receiver<T> for () {
    async fn recv(&mut self) -> Option<T> {
        None
    }
}
