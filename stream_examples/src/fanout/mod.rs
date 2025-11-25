use stream_utils::fanout::{
    self, consumer::FanoutConsumerGroup, source::FanoutSource,
};

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

async fn test_nonsend_channel<Channel>(channel: Channel)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone + 'static,
    Channel::Receiver: 'static,
{
    run(channel.clone()).await; // running the future directly should always work
    let local_set = tokio::task::LocalSet::new();
    let _ = local_set
        .run_until(async {
            tokio::task::spawn_local(run(channel)).await // make sure it works with spawn_local trait bounds
        })
        .await;
}

pub(super) async fn main() {
    test_channel!(tokio::sync::mpsc::channel);
    test_channel!(kanal::bounded_async);
    test_channel!(@nonsend crossfire::spsc::bounded_async); // crossfire spsc sender is !Send
    test_channel!(crossfire::mpsc::bounded_async);
    test_channel!(crossfire::mpmc::bounded_async);
}

async fn run<Channel>(channel: Channel)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone,
    Channel::Receiver: 'static,
{
    // testing that an aribtrary number of consumers works
    let bytes_counter = consumer::bytes_counter::BytesCounter::new();
    let bytes_counter_2 = consumer::bytes_counter::BytesCounter::new_with_limit(10);
    let bufferer = consumer::bufferer::Bufferer;
    let download_fanout_consumers = consumer::DownloadFanoutConsumers::builder()
        .bytes_counter_1(&bytes_counter)
        .bytes_counter_2(&bytes_counter)
        .bytes_counter_3(&bytes_counter)
        .bytes_counter_4(&bytes_counter)
        .bytes_counter_5(&bytes_counter)
        .bytes_counter_6(&bytes_counter)
        .bytes_counter_7(&bytes_counter)
        .bytes_counter_8(&bytes_counter)
        .bytes_counter_9(&bytes_counter)
        .bytes_counter_10(&bytes_counter)
        .bytes_counter_11(&bytes_counter)
        .bytes_counter_12(&bytes_counter)
        .bytes_counter_13(&bytes_counter_2)
        .bufferer(&bufferer)
        .build();

    let mut fanouts = Vec::with_capacity(3);

    fanouts.push(fanout::StreamFanout::new(
        source::Source::from(source::buffer::BytesSource::from(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
            bytes::Bytes::from_static(&[0u8; 20]),
            bytes::Bytes::from_static(&[0u8; 20]),
        ])),
        download_fanout_consumers.clone(),
    ));
    fanouts.push(fanout::StreamFanout::new(
        source::Source::from(source::buffer::BytesSource::from(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
        ])),
        download_fanout_consumers.clone(),
    ));

    fanouts.push(fanout::StreamFanout::new(
        source::Source::from(source::url::UrlSource::from_url(
            "https://thehive.ai/".to_string(),
        )),
        download_fanout_consumers.clone(),
    ));

    let mut i = 0;
    let result = loop {
        println!("try {i}");
        let (outputs, result) = attempt_download(fanouts.into_iter(), channel.clone()).await;
        match result {
            Ok(()) => {
                break Ok(outputs);
            }
            Err((retry_fanouts, download_errors)) => match retry_fanouts {
                Some(retry_fanouts) if i <= 3 => {
                    fanouts = retry_fanouts;
                }
                _ => {
                    if download_errors.is_empty() {
                        break Ok(outputs);
                    } else {
                        break Err(download_errors);
                    }
                }
            },
        }
        i += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(i)).await;
    };
    println!("{result:?}");
}

async fn attempt_download<'a, Channel>(
    fanouts: impl Iterator<
        Item = fanout::ReadyStreamFanout<source::Source, consumer::DownloadFanoutConsumers<'a>>,
    >,
    channel: Channel,
) -> (
    Vec<consumer::DownloadFanoutConsumersOutput<'a>>,
    Result<
        (),
        (
            Option<
                Vec<
                    fanout::ReadyStreamFanout<
                        source::Source,
                        consumer::DownloadFanoutConsumers<'a>,
                    >,
                >,
            >,
            Vec<Error>,
        ),
    >,
)
where
    Channel: stream_utils::channel::Channel<Item = bytes::Bytes> + Clone,
    Channel::Receiver: 'static,
{
    let results = futures::future::join_all(fanouts.into_iter().map(|fanout| {
        fanout
            .into_driver()
            .with_broadcaster_channel(channel.clone())
            .with_broadcaster_buffer_size(1)
            .drive()
    }))
    .await;
    let downloads_count = results.len();
    let mut outputs = Vec::with_capacity(downloads_count);
    let mut retry_fanouts = Vec::with_capacity(downloads_count);
    let mut errors = Vec::with_capacity(downloads_count);
    for (used_download_fanout, result) in results {
        let (source, mut retry_consumers) = used_download_fanout.into_parts();
        let mut retry_source = source.reset();
        match result {
            Ok(download_fanout_output) => {
                if let Ok(buffer) = &download_fanout_output.bufferer {
                    retry_source = Some(source::Source::from(source::buffer::BytesSource::from(
                        buffer.clone(),
                    )));
                    retry_consumers = retry_consumers.retry(&download_fanout_output);
                }
                outputs.push(download_fanout_output);
            }
            Err(error) => {
                errors.push(error);
            }
        }
        if let Some(retry_source) = retry_source {
            retry_fanouts.push(fanout::StreamFanout::new(retry_source, retry_consumers));
        }
    }

    if errors.is_empty() {
        if outputs.iter().any(consumer::DownloadFanoutConsumersOutput::should_retry)
            && outputs.iter().all(consumer::DownloadFanoutConsumersOutput::is_retryable)
        {
            // some downloads failed
            if retry_fanouts.len() < downloads_count {
                // some downloads cannot be retried, don't retry any
                (outputs, Err((None, errors)))
            } else {
                // all downloads can be retried
                (outputs, Err((Some(retry_fanouts), errors)))
            }
        } else {
            // all downloads successful
            (outputs, Ok(()))
        }
    } else {
        // some downloads failed
        if retry_fanouts.len() < downloads_count {
            // some downloads cannot be retried, don't retry any
            (outputs, Err((None, errors)))
        } else {
            // all downloads can be retried
            (outputs, Err((Some(retry_fanouts), errors)))
        }
    }
}

#[derive(Debug)]
pub(crate) struct Error(String);

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
