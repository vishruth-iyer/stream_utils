use crate::channel::{self, sender::Sender};

pub struct DownloadFanoutDownload<
    Source,
    Consumers,
    BroadcasterChannel: channel::Channel<bytes::Bytes>,
    EgressChannel: super::egress::Channel,
    SourceInfo,
> {
    download_fanout: super::DownloadFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    egress_tx: EgressChannel::Sender,
    source_info: std::marker::PhantomData<SourceInfo>,
}

impl<Source, Consumers, BroadcasterChannel, EgressChannel, SourceInfo>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressChannel, SourceInfo>
where
    Source: super::source::FanoutSource + super::source::IntoInfo<SourceInfo>,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<bytes::Bytes>,
    EgressChannel: super::egress::Channel,
{
    pub async fn send<Error>(
        mut self,
    ) -> Result<(SourceInfo, Consumers::Output), (Option<super::DownloadFanout<Source, Consumers>>, Error)>
    where
        super::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        match self
            .download_fanout
            .download_inner::<BroadcasterChannel, EgressChannel, Error>(
                self.broadcaster_channel,
                &self.egress_tx,
            )
            .await
        {
            Ok(download_fanout_output) => {
                drop(self.egress_tx);
                Ok((
                    self.download_fanout.source.into_info(),
                    download_fanout_output,
                ))
            }
            Err(e) => {
                // notify egress receiver that an error occurred
                // for the egress multipart upload use case, this aborts the upload
                let _ = self.egress_tx.send(Err(super::egress::GenericError)).await;
                drop(self.egress_tx);
                let retry_fanout = e.get_retry_fanout(self.download_fanout);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, SourceInfo>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, (), SourceInfo>
where
    BroadcasterChannel: channel::Channel<bytes::Bytes>,
{
    pub fn with_egress_tx<EgressChannel: super::egress::Channel>(
        self,
        egress_tx: EgressChannel::Sender,
    ) -> DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressChannel, SourceInfo>
    {
        DownloadFanoutDownload {
            download_fanout: self.download_fanout,
            broadcaster_channel: self.broadcaster_channel,
            egress_tx,
            source_info: self.source_info,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressChannel>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressChannel, ()>
where
    BroadcasterChannel: channel::Channel<bytes::Bytes>,
    EgressChannel: super::egress::Channel,
{
    pub fn with_source_info<SourceInfo>(
        self,
    ) -> DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressChannel, SourceInfo>
    where
        Source: super::source::IntoInfo<SourceInfo>,
    {
        DownloadFanoutDownload {
            download_fanout: self.download_fanout,
            broadcaster_channel: self.broadcaster_channel,
            egress_tx: self.egress_tx,
            source_info: std::marker::PhantomData,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel: channel::Channel<bytes::Bytes>>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, (), ()>
{
    pub fn new(
        download_fanout: super::DownloadFanout<Source, Consumers>,
        broadcaster_channel: BroadcasterChannel,
    ) -> Self {
        Self {
            download_fanout,
            broadcaster_channel,
            egress_tx: (),
            source_info: std::marker::PhantomData,
        }
    }
}
