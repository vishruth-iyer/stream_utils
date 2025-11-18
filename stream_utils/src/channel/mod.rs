mod impls;
pub mod receiver;
pub mod sender;

pub trait Channel<Item> {
    type Sender: sender::Sender<Item>;
    type Receiver: receiver::Receiver<Item>;

    fn create_channel(&self, buffer_size: usize) -> (Self::Sender, Self::Receiver);
}

impl<T, F, Sender, Receiver> Channel<T> for F
where
    F: Fn(usize) -> (Sender, Receiver),
    Sender: sender::Sender<T>,
    Receiver: receiver::Receiver<T>,
{
    type Sender = Sender;
    type Receiver = Receiver;
    fn create_channel(&self, buffer_size: usize) -> (Self::Sender, Self::Receiver) {
        (self)(buffer_size)
    }
}

impl<T> Channel<T> for () {
    type Sender = ();
    type Receiver = ();

    fn create_channel(&self, _buffer_size: usize) -> (Self::Sender, Self::Receiver) {
        ((), ())
    }
}
