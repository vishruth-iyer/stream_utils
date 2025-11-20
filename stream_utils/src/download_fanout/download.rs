use crate::channel;

pub struct DownloadFanoutDownload<
    Source,
    Consumers,
    BroadcasterChannel,
    EgressSender,
    SourceInfo,
> {
    download_fanout: super::DownloadFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    egress_tx: EgressSender,
    source_info: std::marker::PhantomData<SourceInfo>,
}

async fn send_helper<Source, Consumers, BroadcasterChannel, EgressSender, Error>(
    download_fanout: &mut super::DownloadFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    egress_tx: EgressSender,
) -> Result<Consumers::Output, super::error::DownloadFanoutError<Error>>
where
    Source: super::source::FanoutSource,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    BroadcasterChannel::Receiver: 'static,
    EgressSender: super::egress::Sender,
    super::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
{
    match download_fanout
        .download_inner::<BroadcasterChannel, EgressSender, Error>(broadcaster_channel, &egress_tx)
        .await
    {
        Ok(download_fanout_output) => {
            drop(egress_tx);
            Ok(download_fanout_output)
        }
        Err(e) => {
            // notify egress receiver that an error occurred
            // for the egress multipart upload use case, this aborts the upload
            let _ = egress_tx.send(super::egress::GenericError.into()).await;
            drop(egress_tx);
            Err(e)
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressSender>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender, ()>
where
    Source: super::source::FanoutSource,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    BroadcasterChannel::Receiver: 'static,
    EgressSender: super::egress::Sender,
{
    pub async fn send<Error>(
        mut self,
    ) -> Result<Consumers::Output, (Option<super::DownloadFanout<Source, Consumers>>, Error)>
    where
        super::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        match send_helper(
            &mut self.download_fanout,
            self.broadcaster_channel,
            self.egress_tx,
        )
        .await
        {
            Ok(download_fanout_output) => Ok(download_fanout_output),
            Err(e) => {
                let retry_fanout = e.get_retry_fanout(self.download_fanout);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressSender, SourceInfo>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender, SourceInfo>
where
    Source: super::source::FanoutSource + super::source::IntoInfo<SourceInfo>,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    BroadcasterChannel::Receiver: 'static,
    EgressSender: super::egress::Sender,
{
    pub async fn send_returning_source_info<Error>(
        mut self,
    ) -> Result<
        (SourceInfo, Consumers::Output),
        (Option<super::DownloadFanout<Source, Consumers>>, Error),
    >
    where
        super::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        match send_helper(
            &mut self.download_fanout,
            self.broadcaster_channel,
            self.egress_tx,
        )
        .await
        {
            Ok(download_fanout_output) => Ok((
                self.download_fanout.source.into_info(),
                download_fanout_output,
            )),
            Err(e) => {
                let retry_fanout = e.get_retry_fanout(self.download_fanout);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, SourceInfo>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, (), SourceInfo>
{
    pub fn with_egress_tx<EgressSender>(
        self,
        egress_tx: EgressSender,
    ) -> DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender, SourceInfo>
    where
        EgressSender: super::egress::Sender,
    {
        DownloadFanoutDownload {
            download_fanout: self.download_fanout,
            broadcaster_channel: self.broadcaster_channel,
            egress_tx,
            source_info: self.source_info,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressSender>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender, ()>
{
    pub fn with_source_info<SourceInfo>(
        self,
    ) -> DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender, SourceInfo>
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

impl<Source, Consumers, BroadcasterChannel>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, channel::sender::NoOpSender<super::egress::Item>, ()>
{
    pub(crate) fn new(
        download_fanout: super::DownloadFanout<Source, Consumers>,
        broadcaster_channel: BroadcasterChannel,
    ) -> Self {
        Self {
            download_fanout,
            broadcaster_channel,
            egress_tx: channel::sender::NoOpSender::new(),
            source_info: std::marker::PhantomData,
        }
    }
}
