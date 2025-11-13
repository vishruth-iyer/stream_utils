#[derive(thiserror::Error, Debug)]
#[error("an error occurred")]
pub struct GenericError;

#[derive(thiserror::Error, Debug)]
pub enum DownloadFanoutError<E> {
    #[error("[RetryableError]: {0}")]
    RetryableError(E),
    #[error("[NonRetryableError]: {0}")]
    NonRetryableError(E),
}

impl<E> DownloadFanoutError<E> {
    pub fn into_inner(self) -> E {
        match self {
            Self::RetryableError(e) => e,
            Self::NonRetryableError(e) => e,
        }
    }

    pub fn get_retry_fanout<Source, Consumers>(
        &self,
        download_fanout: super::DownloadFanout<Source, Consumers>,
    ) -> Option<super::DownloadFanout<Source, Consumers>>
    where
        Source: super::source::FanoutSource,
    {
        match self {
            Self::RetryableError(_) => {
                download_fanout
                    .source
                    .reset()
                    .map(|source| super::DownloadFanout {
                        source,
                        consumers: download_fanout.consumers,
                    })
            }
            Self::NonRetryableError(_) => None,
        }
    }
}
