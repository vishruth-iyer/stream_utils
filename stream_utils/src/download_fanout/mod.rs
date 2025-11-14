use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::info;

use crate::{
    broadcaster,
    download_fanout::{
        error::{DownloadFanoutError, GenericError},
        source::FanoutSource,
    },
};

pub mod consumer;
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
}

impl<Source, Consumers> DownloadFanout<Source, Consumers>
where
    Source: FanoutSource,
    Consumers: consumer::FanoutConsumerGroup,
{
    pub async fn download<Error>(mut self) -> Result<Consumers::Output, (Option<Self>, Error)>
    where
        DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        match self.download_inner(None).await {
            Ok(download_fanout_output) => Ok(download_fanout_output),
            Err(e) => {
                let retry_fanout = e.get_retry_fanout(self);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }

    /// Enables download with a separate mpsc channel to output downloaded bytes to.
    /// Allows for easy extension of the download fanout.
    pub async fn download_with_egress<Error>(
        mut self,
        egress_tx: mpsc::Sender<Result<Bytes, GenericError>>,
    ) -> Result<Consumers::Output, (Option<Self>, Error)>
    where
        DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        match self.download_inner(Some(&egress_tx)).await {
            Ok(download_fanout_output) => {
                drop(egress_tx);
                Ok(download_fanout_output)
            }
            Err(e) => {
                // notify egress receiver that an error occurred
                // for the egress multipart upload use case, this aborts the upload
                let _ = egress_tx.send(Err(GenericError)).await;
                drop(egress_tx);
                let retry_fanout = e.get_retry_fanout(self);
                Err((retry_fanout, e.into_inner()))
            }
        }
    }

    #[tracing::instrument(name = "download_fanout", skip_all)]
    async fn download_inner<Error>(
        &mut self,
        egress_tx: Option<&mpsc::Sender<Result<Bytes, GenericError>>>,
    ) -> Result<Consumers::Output, DownloadFanoutError<Error>>
    where
        DownloadFanoutError<Error>: From<Source::Error> + From<Consumers::Error>,
    {
        let start = tokio::time::Instant::now();

        let content_length = self.source.get_content_length().await?;

        let mut download_broadcaster = broadcaster::Broadcaster::new();

        // create subscriber futures
        let subscribers_future = self
            .consumers
            .consume_from_fanout(&mut download_broadcaster, content_length);
        let egress_future = send_to_egress(egress_tx, download_broadcaster.subscribe());

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

async fn send_to_egress(
    egress_tx: Option<&mpsc::Sender<Result<Bytes, GenericError>>>,
    mut rx: broadcaster::channel::Receiver<Bytes>,
) {
    if let Some(egress_tx) = egress_tx {
        while let Some(bytes) = rx.recv().await {
            if let Err(_) = egress_tx.send(Ok(bytes)).await {
                break;
            }
        }
    }

    drop(rx); // make sure rx is moved into this future and dropped
}
