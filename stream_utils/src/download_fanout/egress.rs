pub struct Item(Result<bytes::Bytes, GenericError>);

impl Item {
    pub fn into_inner(self) -> Result<bytes::Bytes, GenericError> {
        self.0
    }
}

impl From<Result<bytes::Bytes, GenericError>> for Item {
    fn from(value: Result<bytes::Bytes, GenericError>) -> Self {
        Self(value)
    }
}

impl From<bytes::Bytes> for Item {
    fn from(value: bytes::Bytes) -> Self {
        Self(Ok(value))
    }
}

impl From<GenericError> for Item {
    fn from(value: GenericError) -> Self {
        Self(Err(value))
    }
}

impl From<Item> for Result<bytes::Bytes, GenericError> {
    fn from(value: Item) -> Self {
        value.into_inner()
    }
}

impl std::ops::Deref for Item {
    type Target = Result<bytes::Bytes, GenericError>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait Sender: crate::channel::sender::Sender<Item> {}
impl<T> Sender for T where T: crate::channel::sender::Sender<Item> {}

#[derive(thiserror::Error, Debug)]
#[error("an error occurred")]
pub struct GenericError;
