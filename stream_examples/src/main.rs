use stream_utils::download_fanout;

#[tokio::main]
async fn main() {
    let download_fanout_consumers = DownloadFanoutConsumers::builder()
        .bytes_counter_1(BytesCounter::new())
        .bytes_counter_2(BytesCounter::new_with_limit(32768))
        .bytes_counter_3(BytesCounter::new_with_limit(16384))
        .bytes_counter_4(BytesCounter::new_with_limit(8192))
        .bytes_counter_5(BytesCounter::new_with_limit(4096))
        .bytes_counter_6(BytesCounter::new_with_limit(2048))
        .bytes_counter_7(BytesCounter::new_with_limit(1024))
        .bytes_counter_8(BytesCounter::new_with_limit(512))
        .bytes_counter_9(BytesCounter::new_with_limit(256))
        .bytes_counter_10(BytesCounter::new_with_limit(128))
        .bytes_counter_11(BytesCounter::new_with_limit(64))
        .bytes_counter_12(BytesCounter::new_with_limit(32))
        .bytes_counter_13(BytesCounter::new_with_limit(16))
        .build();
    let downloader_1 = download_fanout::DownloadFanout::new(
        BytesSource(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
            bytes::Bytes::from_static(&[0u8; 20]),
            bytes::Bytes::from_static(&[0u8; 20]),
        ]),
        download_fanout_consumers.clone(),
    );
    let downloader_2 = download_fanout::DownloadFanout::new(
        BytesSource(vec![
            bytes::Bytes::from_static(&[0u8; 10]),
        ]),
        download_fanout_consumers,
    );

    let mut fanouts = vec![downloader_1, downloader_2];
    let mut i = 0;
    let result = loop {
        println!("try {i}");
        match attempt_download(fanouts.into_iter()).await {
            Ok(outputs) => {
                break Ok(outputs);
            }
            Err((retry_fanouts, download_errors)) => {
                match retry_fanouts {
                    Some(retry_fanouts) if i <= 3 => {
                        fanouts = retry_fanouts;
                    }
                    _ => {
                        break Err(download_errors);
                    }
                }
            }
        }
        i += 1;
        tokio::time::sleep(tokio::time::Duration::from_secs(i)).await;
    };
    println!("{result:?}");
}

async fn attempt_download<Source, Consumers, Error>(fanouts: impl Iterator<Item = download_fanout::DownloadFanout<Source, Consumers>>) -> Result<Vec<Consumers::Output>, (Option<Vec<download_fanout::DownloadFanout<Source, Consumers>>>, Vec<Error>)>
where
    Source: download_fanout::source::FanoutSource,
    Consumers: download_fanout::consumer::FanoutConsumerGroup,
    download_fanout::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
{
    let results = futures::future::join_all(fanouts.into_iter().map(|fanout| fanout.download())).await;
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
    async fn consume_from_fanout(
        &self,
        mut rx: stream_utils::broadcaster::channel::Receiver<bytes::Bytes>,
        cancellation_token: stream_utils::broadcaster::CancellationToken,
        _content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error> {
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
    fn get_original_url(&self) -> Option<&str> {
        None
    }
    async fn get_content_length(&mut self) -> Result<Option<u64>, Self::Error> {
        Ok(Some(
            self.0.iter().map(|bytes| bytes.len()).sum::<usize>() as u64
        ))
    }
    async fn broadcast(
        &mut self,
        broadcaster: stream_utils::broadcaster::Broadcaster<bytes::Bytes>,
    ) -> Result<(), Self::Error> {
        for chunk in &self.0 {
            let _ = broadcaster.broadcast(chunk.clone()).await;
        }
        Ok(())
    }
    fn reset(self) -> Option<Self> {
        Some(self)
    }
}

#[derive(Debug)]
struct Error(String);

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

#[derive(bon::Builder, proc_macros::FanoutConsumerGroup, Clone)]
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
