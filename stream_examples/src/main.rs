use stream_utils::download_fanout;

mod consumer;
mod source;

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
    run(channel.clone()).await; // running the future directly should always work
    let local_set = tokio::task::LocalSet::new();
    let _ = local_set.run_until(async {
        tokio::task::spawn_local(run(channel)).await // make sure it works with spawn_local trait bounds
    }).await;
}

async fn run<Channel>(channel: Channel)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone,
    Channel::Receiver: 'static,
{
    // testing that an aribtrary number of consumers works
    let download_fanout_consumers = consumer::DownloadFanoutConsumers::builder()
        .bytes_counter_1(consumer::BytesCounter::new())
        .bytes_counter_2(consumer::BytesCounter::new())
        .bytes_counter_3(consumer::BytesCounter::new())
        .bytes_counter_4(consumer::BytesCounter::new())
        .bytes_counter_5(consumer::BytesCounter::new())
        .bytes_counter_6(consumer::BytesCounter::new())
        .bytes_counter_7(consumer::BytesCounter::new())
        .bytes_counter_8(consumer::BytesCounter::new())
        .bytes_counter_9(consumer::BytesCounter::new())
        .bytes_counter_10(consumer::BytesCounter::new())
        .bytes_counter_11(consumer::BytesCounter::new())
        .bytes_counter_12(consumer::BytesCounter::new())
        .bytes_counter_13(consumer::BytesCounter::new())
        .build();
    let downloader_1 = download_fanout::DownloadFanout::new(
        source::Source::from(source::BytesSource::from(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
            bytes::Bytes::from_static(&[0u8; 20]),
            bytes::Bytes::from_static(&[0u8; 20]),
        ])),
        &download_fanout_consumers,
    );
    let downloader_2 = download_fanout::DownloadFanout::new(
        source::Source::from(source::BytesSource::from(vec![bytes::Bytes::from_static(&[0u8; 10])])),
        &download_fanout_consumers,
    );

    let downloader_3 = download_fanout::DownloadFanout::new(
        source::Source::from(source::UrlSource::from_url("https://thehive.ai/".to_string())),
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
