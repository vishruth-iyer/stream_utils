use tracing::info;

use crate::{
    broadcaster, channel,
    download_fanout::{error::DownloadFanoutError, source::FanoutSource},
};

pub mod consumer;
pub mod download;
pub mod egress;
pub mod error;
pub mod source;

pub struct DownloadFanout<Source, Consumers> {
    source: Source,
    consumers: Consumers,
}

impl<Source, Consumers> DownloadFanout<Source, Consumers> {
    pub fn new(source: Source, consumers: Consumers) -> Self {
        Self { source, consumers }
    }

    pub fn get_source_info<'a, SourceInfo>(&'a self) -> SourceInfo
    where
        Source: source::GetInfo<'a, SourceInfo>,
        SourceInfo: 'a,
    {
        self.source.get_info()
    }

    pub fn download<BroadcasterChannel>(
        self,
        broadcaster_channel: BroadcasterChannel,
    ) -> download::DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, channel::sender::NoOpSender<egress::Item>, ()>
    where
        BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    {
        download::DownloadFanoutDownload::new(self, broadcaster_channel)
    }
}

impl<Source, Consumers> DownloadFanout<Source, Consumers>
where
    Source: FanoutSource,
    Consumers: consumer::FanoutConsumerGroup,
{
    #[tracing::instrument(name = "download_fanout", skip_all)]
    async fn download_inner<BroadcasterChannel, EgressSender, Error>(
        &mut self,
        broadcaster_channel: BroadcasterChannel,
        egress_tx: &EgressSender,
    ) -> Result<Consumers::Output, DownloadFanoutError<Error>>
    where
        BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
        BroadcasterChannel::Receiver: 'static,
        EgressSender: egress::Sender,
        DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        let start = tokio::time::Instant::now();

        let content_length = self.source.get_content_length().await?;

        let mut download_broadcaster = broadcaster::Broadcaster::builder()
            .channel(broadcaster_channel)
            .build();

        // create subscriber futures
        let subscribers_future = self
            .consumers
            .consume_from_fanout(&mut download_broadcaster, content_length);
        let egress_future = egress_tx.send_from_broadcaster(&mut download_broadcaster);

        // broadcast download stream
        let download_broadcast_future = self.source.broadcast(download_broadcaster);

        // poll futures concurrently
        let (download_result, subscriber_output, _) =
            tokio::join!(download_broadcast_future, subscribers_future, egress_future);

        info!(
            response_time = start.elapsed().as_millis(),
            "done downloading bytes and sending to subscribers",
        );

        // propagate errors
        download_result?;
        let download_fanout_output = subscriber_output?;

        Ok(download_fanout_output)
    }
}
