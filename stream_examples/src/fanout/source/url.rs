use stream_utils::fanout;

pub(crate) struct UrlSource {
    url: String,
    response: Option<reqwest::Response>,
}

impl UrlSource {
    pub(crate) fn from_url(url: String) -> Self {
        Self {
            url,
            response: None,
        }
    }
}

impl fanout::source::FanoutSource for UrlSource {
    type Item = bytes::Bytes;
    type Error = super::super::Error;
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
            .broadcast_from_result_stream(response.bytes_stream())
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
