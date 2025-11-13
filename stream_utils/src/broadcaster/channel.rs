use tokio::sync::mpsc;

pub(super) fn new<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    (tx.into(), rx.into())
}

pub enum SendResult {
    Success,
    Failure,
}

#[derive(Debug)]
pub struct Sender<T>(mpsc::Sender<T>);

impl<T> Sender<T> {
    pub async fn send(&self, value: T) -> SendResult {
        match self.0.send(value).await {
            Ok(_) => SendResult::Success,
            Err(_) => SendResult::Failure,
        }
    }
}

impl<T> From<mpsc::Sender<T>> for Sender<T> {
    fn from(value: mpsc::Sender<T>) -> Self {
        Self(value)
    }
}

impl<T> std::ops::Deref for Sender<T> {
    type Target = mpsc::Sender<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct Receiver<T>(mpsc::Receiver<T>);

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }
}

impl<T> From<mpsc::Receiver<T>> for Receiver<T> {
    fn from(value: mpsc::Receiver<T>) -> Self {
        Self(value)
    }
}

impl<T> std::ops::Deref for Receiver<T> {
    type Target = mpsc::Receiver<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> std::ops::DerefMut for Receiver<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
