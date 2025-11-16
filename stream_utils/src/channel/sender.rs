pub trait Sender<T> {
    async fn send(&self, item: T) -> Result;
}

impl<T> Sender<T> for () {
    async fn send(&self, _item: T) -> Result {
        Result::Failure
    }
}

impl<T> Sender<T> for tokio::sync::mpsc::Sender<T> {
    async fn send(&self, item: T) -> Result {
        match self.send(item).await {
            Ok(_) => Result::Success,
            Err(_) => Result::Failure,
        }
    }
}

pub enum Result {
    Success,
    Failure,
}
