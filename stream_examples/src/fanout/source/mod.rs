use stream_utils::fanout;

pub mod buffer;
pub mod url;

pub(super) enum Source {
    Bytes(buffer::BytesSource),
    Url(url::UrlSource),
}

impl From<buffer::BytesSource> for Source {
    fn from(value: buffer::BytesSource) -> Self {
        Self::Bytes(value)
    }
}

impl From<url::UrlSource> for Source {
    fn from(value: url::UrlSource) -> Self {
        Self::Url(value)
    }
}

impl fanout::source::FanoutSource for Source {
    type Item = bytes::Bytes;
    type Error = super::Error;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        match self {
            Self::Bytes(bytes_source) => bytes_source.get_content_length().await,
            Self::Url(url_source) => url_source.get_content_length().await,
        }
    }
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: stream_utils::broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: stream_utils::channel::Channel<Item = bytes::Bytes>,
    {
        match self {
            Self::Bytes(bytes_source) => bytes_source.broadcast(broadcaster).await,
            Self::Url(url_source) => url_source.broadcast(broadcaster).await,
        }
    }
    fn reset(self) -> Option<Self> {
        match self {
            Self::Bytes(bytes_source) => bytes_source.reset().map(Self::Bytes),
            Self::Url(url_source) => url_source.reset().map(Self::Url),
        }
    }
}
