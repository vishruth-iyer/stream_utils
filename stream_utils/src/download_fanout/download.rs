use crate::channel;

pub struct DownloadFanoutDownload<
    Source,
    Consumers,
    BroadcasterChannel,
    EgressSender,
> {
    download_fanout: super::DownloadFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    egress_tx: EgressSender,
}

impl<Source, Consumers, BroadcasterChannel>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, ()>
{
    pub(crate) fn new(
        download_fanout: super::DownloadFanout<Source, Consumers>,
        broadcaster_channel: BroadcasterChannel,
    ) -> Self {
        Self {
            download_fanout,
            broadcaster_channel,
            egress_tx: (),
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, OldEgressSender>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, OldEgressSender>
{
    pub fn with_egress_tx<EgressItem, EgressSender>(
        self,
        egress_tx: EgressSender,
    ) -> DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender>
    where
        EgressSender: channel::sender::Sender<Item = EgressItem>,
        EgressItem: super::egress::EgressItem,
    {
        DownloadFanoutDownload {
            download_fanout: self.download_fanout,
            broadcaster_channel: self.broadcaster_channel,
            egress_tx,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressItem, EgressSender>
    DownloadFanoutDownload<Source, Consumers, BroadcasterChannel, EgressSender>
where
    Source: super::source::FanoutSource,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    BroadcasterChannel::Receiver: 'static,
    EgressItem: super::egress::EgressItem,
    EgressSender: super::egress::EgressSender<Item = EgressItem>,
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

    pub async fn send_returning_source_info<SourceInfo, Error>(
        mut self,
    ) -> Result<
        (SourceInfo, Consumers::Output),
        (Option<super::DownloadFanout<Source, Consumers>>, Error),
    >
    where
        SourceInfo: From<Source>,
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
                SourceInfo::from(self.download_fanout.source),
                download_fanout_output,
            )),
            Err(e) => {
                let retry_fanout = e.get_retry_fanout(self.download_fanout);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }
}

async fn send_helper<Source, Consumers, BroadcasterChannel, EgressItem, EgressSender, Error>(
    download_fanout: &mut super::DownloadFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    egress_tx: EgressSender,
) -> Result<Consumers::Output, super::error::DownloadFanoutError<Error>>
where
    Source: super::source::FanoutSource,
    Consumers: super::consumer::FanoutConsumerGroup,
    BroadcasterChannel: channel::Channel<Item = bytes::Bytes>,
    BroadcasterChannel::Receiver: 'static,
    EgressItem: super::egress::EgressItem,
    EgressSender: super::egress::EgressSender<Item = EgressItem>,
    super::error::DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
{
    match download_fanout
        .download_inner::<BroadcasterChannel, EgressItem, EgressSender, Error>(broadcaster_channel, &egress_tx)
        .await
    {
        Ok(download_fanout_output) => {
            drop(egress_tx);
            Ok(download_fanout_output)
        }
        Err(e) => {
            // notify egress receiver that an error occurred
            // for the egress multipart upload use case, this aborts the upload
            let _ = egress_tx.send(EgressItem::error()).await;
            drop(egress_tx);
            Err(e)
        }
    }
}
