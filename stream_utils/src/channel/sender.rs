pub trait Sender {
    type Item;
    async fn send(&self, item: Self::Item) -> Result;
}

pub struct NoOpSender<T> {
    item: std::marker::PhantomData<T>,
}

impl<T> NoOpSender<T> {
    pub fn new() -> Self {
        Self {
            item: std::marker::PhantomData,
        }
    }
}

impl<T> Sender for NoOpSender<T> {
    type Item = T;
    async fn send(&self, _item: Self::Item) -> Result {
        Result::Failure
    }
}

pub enum Result {
    Success,
    Failure,
}
