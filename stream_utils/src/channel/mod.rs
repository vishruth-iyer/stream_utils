mod impls;
pub mod receiver;
pub mod sender;

pub trait Channel {
    type Item;
    type Sender: sender::Sender<Item = Self::Item>;
    type Receiver: receiver::Receiver<Item = Self::Item>;

    fn create_channel(&self, buffer_size: usize) -> (Self::Sender, Self::Receiver);
}

impl<T, F, Sender, Receiver> Channel for F
where
    F: Fn(usize) -> (Sender, Receiver),
    Sender: sender::Sender<Item = T>,
    Receiver: receiver::Receiver<Item = T>,
{
    type Item = T;
    type Sender = Sender;
    type Receiver = Receiver;
    fn create_channel(&self, buffer_size: usize) -> (Self::Sender, Self::Receiver) {
        (self)(buffer_size)
    }
}

pub struct NoOpChannel<T> {
    item: std::marker::PhantomData<T>,
}

impl<T> Channel for NoOpChannel<T> {
    type Item = T;
    type Sender = sender::NoOpSender<Self::Item>;
    type Receiver = receiver::NoOpReceiver<Self::Item>;
    fn create_channel(&self, _buffer_size: usize) -> (Self::Sender, Self::Receiver) {
        (sender::NoOpSender::new(), receiver::NoOpReceiver::new())
    }
}
