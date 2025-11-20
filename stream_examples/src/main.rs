use stream_utils::download_fanout;

macro_rules! test_channel {
    (@nonsend $channel:path) => {
        test_nonsend_channel($channel).await;
    };
    ($channel:path) => {
        test_channel!(@nonsend $channel);
        let _ = tokio::task::spawn(run($channel)).await; // make sure it works with spawn trait bounds
    };
}

#[tokio::main]
async fn main() {
    test_channel!(tokio::sync::mpsc::channel);
    test_channel!(kanal::bounded_async);
    test_channel!(@nonsend crossfire::spsc::bounded_async); // crossfire spsc sender is !Send
    test_channel!(crossfire::mpsc::bounded_async);
    test_channel!(crossfire::mpmc::bounded_async);
}

async fn test_nonsend_channel<Channel>(channel: Channel)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone + 'static,
    Channel::Receiver: 'static,
{
    run(channel.clone()).await; // base should always work
    let local_set = tokio::task::LocalSet::new();
    let _ = local_set.run_until(tokio::task::spawn_local(run(channel))).await; // make sure it works with spawn_local trait bounds
}

async fn run<Channel>(channel: Channel)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone,
    Channel::Receiver: 'static,
{
    let download_fanout_consumers = DownloadFanoutConsumers::builder()
        .bytes_counter_1(BytesCounter::new())
        .bytes_counter_2(BytesCounter::new())
        .bytes_counter_3(BytesCounter::new())
        .bytes_counter_4(BytesCounter::new())
        .bytes_counter_5(BytesCounter::new())
        .bytes_counter_6(BytesCounter::new())
        .bytes_counter_7(BytesCounter::new())
        .bytes_counter_8(BytesCounter::new())
        .bytes_counter_9(BytesCounter::new())
        .bytes_counter_10(BytesCounter::new())
        .bytes_counter_11(BytesCounter::new())
        .bytes_counter_12(BytesCounter::new())
        .bytes_counter_13(BytesCounter::new())
        .build();
    let downloader_1 = download_fanout::DownloadFanout::new(
        Source::from(BytesSource(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
            bytes::Bytes::from_static(&[0u8; 20]),
            bytes::Bytes::from_static(&[0u8; 20]),
        ])),
        &download_fanout_consumers,
    );
    let downloader_2 = download_fanout::DownloadFanout::new(
        Source::from(BytesSource(vec![bytes::Bytes::from_static(&[0u8; 10])])),
        &download_fanout_consumers,
    );

    let downloader_3 = download_fanout::DownloadFanout::new(
        Source::from(UrlSource::from_url("https://thehive.ai/".to_string())),
        &download_fanout_consumers,
    );

    let mut fanouts = vec![downloader_1, downloader_2, downloader_3];
    let mut i = 0;
    let result = loop {
        match attempt_download(fanouts.into_iter(), channel.clone()).await {
            Ok(outputs) => {
                break Ok(outputs);
            }
            Err((retry_fanouts, download_errors)) => match retry_fanouts {
                Some(retry_fanouts) if i <= 3 => {
                    fanouts = retry_fanouts;
                }
                _ => {
                    break Err(download_errors);
                }
            },
        }
        i += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(i)).await;
    };
    assert!(result.is_ok());
}

async fn attempt_download<Source, Consumers, Channel, Error>(
    fanouts: impl Iterator<Item = download_fanout::DownloadFanout<Source, Consumers>>,
    channel: Channel,
) -> Result<
    Vec<Consumers::Output>,
    (
        Option<Vec<download_fanout::DownloadFanout<Source, Consumers>>>,
        Vec<Error>,
    ),
>
where
    Source: download_fanout::source::FanoutSource,
    Consumers: download_fanout::consumer::FanoutConsumerGroup,
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone,
    Channel::Receiver: 'static,
    download_fanout::error::DownloadFanoutError<Error>:
        From<Source::Error> + From<Consumers::Error>,
{
    let results = futures::future::join_all(
        fanouts
            .into_iter()
            .map(|fanout| fanout.download(channel.clone()).send()),
    )
    .await;
    let downloads_count = results.len();
    let mut outputs = Vec::with_capacity(downloads_count);
    let mut retry_fanouts = Vec::with_capacity(downloads_count);
    let mut errors = Vec::with_capacity(downloads_count);
    for result in results {
        match result {
            Ok(download_fanout_output) => {
                outputs.push(download_fanout_output);
            }
            Err((retry_fanout, error)) => {
                if let Some(retry_fanout) = retry_fanout {
                    retry_fanouts.push(retry_fanout);
                }
                errors.push(error);
            }
        }
    }

    if errors.is_empty() {
        // all downloads successful
        Ok(outputs)
    } else {
        // some downloads failed
        if retry_fanouts.len() < downloads_count {
            // some downloads cannot be retried, don't retry any
            Err((None, errors))
        } else {
            // all downloads can be retried
            Err((Some(retry_fanouts), errors))
        }
    }
}

#[derive(Clone, Copy)]
struct BytesCounter {
    limit: usize,
}

impl BytesCounter {
    fn new() -> Self {
        Self::new_with_limit(usize::MAX)
    }
    fn new_with_limit(limit: usize) -> Self {
        Self { limit }
    }
}

impl download_fanout::consumer::FanoutConsumer for BytesCounter {
    type Output = usize;
    type Error = Error;
    async fn consume_from_fanout<Rx>(
        &self,
        mut rx: Rx,
        cancellation_token: stream_utils::broadcaster::CancellationToken,
        _content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>
    where
        Rx: stream_utils::channel::receiver::Receiver<Item = bytes::Bytes>,
    {
        let mut i = 0;
        while let Some(bytes) = rx.recv().await {
            i += bytes.len();
            if i > self.limit {
                cancellation_token.cancel();
                return Err(Error("too big".to_string()));
            }
        }
        Ok(i)
    }
}

struct BytesSource(Vec<bytes::Bytes>);

impl download_fanout::source::FanoutSource for BytesSource {
    type Error = Error;
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

struct UrlSource {
    url: String,
    response: Option<reqwest::Response>,
}

impl UrlSource {
    fn from_url(url: String) -> Self {
        Self {
            url,
            response: None,
        }
    }
}

impl download_fanout::source::FanoutSource for UrlSource {
    type Error = Error;
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

enum Source {
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
    type Error = Error;
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

impl From<UrlSource> for String {
    fn from(value: UrlSource) -> Self {
        value.url
    }
}

#[derive(Debug)]
struct Error(String);

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Error> for download_fanout::error::DownloadFanoutError<Error> {
    fn from(value: Error) -> Self {
        Self::RetryableError(value)
    }
}

#[derive(bon::Builder, stream_utils::proc_macros::FanoutConsumerGroup, Clone)]
#[fanout_consumer_group_error_ty(Error)]
#[fanout_consumer_group_output_derive(Debug)]
struct DownloadFanoutConsumers {
    #[builder(into)]
    bytes_counter_1: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_2: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_3: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_4: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_5: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_6: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_7: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_8: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_9: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_10: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_11: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_12: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
    #[builder(into)]
    bytes_counter_13: download_fanout::consumer::ConsumerOrResolved<BytesCounter>,
}
