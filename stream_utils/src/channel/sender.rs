pub trait Sender<T> {
    async fn send(&self, item: T) -> Result;
}

impl<T> Sender<T> for () {
    async fn send(&self, _item: T) -> Result {
        Result::Failure
    }
}

pub enum Result {
    Success,
    Failure,
}
