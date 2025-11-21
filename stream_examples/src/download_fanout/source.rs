use stream_utils::download_fanout;

pub(super) struct BytesSource(Vec<bytes::Bytes>);

impl From<Vec<bytes::Bytes>> for BytesSource {
    fn from(value: Vec<bytes::Bytes>) -> Self {
        Self(value)
    }
}

impl download_fanout::source::FanoutSource for BytesSource {
    type Error = super::Error;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        Ok(Some(
            self.0.iter().map(|bytes| bytes.len()).sum::<usize>() as u64
        ))
    }
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: stream_utils::broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: stream_utils::channel::Channel<Item = bytes::Bytes>,
    {
        for chunk in &self.0 {
            let _ = broadcaster.broadcast(chunk.clone()).await;
        }
        Ok(())
    }
    fn reset(self) -> Option<Self> {
        Some(self)
    }
}

pub(super) struct UrlSource {
    url: String,
    response: Option<reqwest::Response>,
}

impl UrlSource {
    pub(super) fn from_url(url: String) -> Self {
        Self {
            url,
            response: None,
        }
    }
}

impl download_fanout::source::FanoutSource for UrlSource {
    type Error = super::Error;
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        if let Some(response) = &self.response {
            return Ok(response.content_length());
        }
        let response = reqwest::Client::new()
            .get(&self.url)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let content_length = response.content_length();
        self.response = Some(response);
        Ok(content_length)
    }
    async fn broadcast<Channel>(
        &mut self,
        broadcaster: stream_utils::broadcaster::Broadcaster<Channel>,
    ) -> Result<(), Self::Error>
    where
        Channel: stream_utils::channel::Channel<Item = bytes::Bytes>,
    {
        let response = match self.response.take() {
            Some(response) => response,
            None => reqwest::Client::new()
                .get(&self.url)
                .send()
                .await
                .map_err(|e| e.to_string())?,
        };
        broadcaster
            .broadcast_from_stream(response.bytes_stream())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    fn reset(self) -> Option<Self> {
        Some(Self {
            url: self.url,
            response: None,
        })
    }
}

pub(super) enum Source {
    Bytes(BytesSource),
    Url(UrlSource),
}

impl From<BytesSource> for Source {
    fn from(value: BytesSource) -> Self {
        Self::Bytes(value)
    }
}

impl From<UrlSource> for Source {
    fn from(value: UrlSource) -> Self {
        Self::Url(value)
    }
}

impl download_fanout::source::FanoutSource for Source {
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
