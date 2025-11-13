use bytes::Bytes;

use crate::broadcaster;

pub trait FanoutConsumer {
    type Output;
    type Error;
    async fn consume_from_fanout(
        &self,
        rx: broadcaster::channel::Receiver<Bytes>,
        cancellation_token: broadcaster::CancellationToken,
        content_length: Option<u64>,
    ) -> Result<Self::Output, Self::Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum ConsumerOrResolved<Consumer: FanoutConsumer> {
    Consumer(Consumer),
    ConsumerOutput(Consumer::Output),
}

impl<Consumer: FanoutConsumer> From<Consumer> for ConsumerOrResolved<Consumer> {
    fn from(value: Consumer) -> Self {
        Self::Consumer(value)
    }
}

// impl<Consumer> Clone for ConsumerOrResolved<Consumer>
// where
//     Consumer: FanoutConsumer + Clone,
//     Consumer::Output: Clone
// {
//     fn clone(&self) -> Self {
//         match self {
//             Self::Consumer(consumer) => Self::Consumer(consumer.clone()),
//             Self::ConsumerOutput(consumer_output) => Self::ConsumerOutput(consumer_output.clone()),
//         }
//     }
// }

// impl<Consumer: FanoutConsumer> FanoutConsumer for ConsumerOrResolved<Consumer>
// where
//     Consumer::Output: Clone,
// {
//     type Output = Consumer::Output;
//     type Error = Consumer::Error;
//     async fn consume_from_fanout(
//         &self,
//         rx: broadcaster::channel::Receiver<Bytes>,
//         cancellation_token: broadcaster::CancellationToken,
//         content_length: Option<u64>,
//     ) -> Result<Self::Output, Self::Error> {
//         match self {
//             ConsumerOrResolved::Consumer(consumer) => consumer.consume_from_fanout(rx, cancellation_token, content_length).await,
//             ConsumerOrResolved::ConsumerOutput(consumer_output) => Ok(consumer_output.clone()),
//         }
//     }
// }

pub trait FanoutConsumerGroup {
    type Output;
    type Error;
    fn consume_from_fanout(
        &self,
        download_broadcaster: &mut broadcaster::Broadcaster<Bytes>,
        content_length: Option<u64>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + '_ {
        self._consume_from_fanout(download_broadcaster, content_length).1
    }
    // the above is the only signature I want, but we need the below to tell the compiler that the future doesn't borrow the broadcaster
    fn _consume_from_fanout<'a, 'b>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Bytes>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Bytes>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    );
}

// impl<Consumer> FanoutConsumerGroup for Consumer
// where
//     Consumer: FanoutConsumer
// {
//     type Output = Consumer::Output;
//     type Error = Consumer::Error;
//     fn _consume_from_fanout<'a, 'b>(
//         &'a self,
//         download_broadcaster: &'b mut broadcaster::Broadcaster<Bytes>,
//         content_length: Option<u64>,
//     ) -> (
//         &'b mut broadcaster::Broadcaster<Bytes>,
//         impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
//     ) {
//         let future = self.consume_from_fanout(download_broadcaster.subscribe(), download_broadcaster.get_cancellation_token().clone(), content_length);
//         (download_broadcaster, future)
//     }
// }

impl<Consumer> FanoutConsumerGroup for ConsumerOrResolved<Consumer>
where
    Consumer: FanoutConsumer,
    Consumer::Output: Clone,
{
    type Output = Consumer::Output;
    type Error = Consumer::Error;
    fn _consume_from_fanout<'a, 'b>(
        &'a self,
        download_broadcaster: &'b mut broadcaster::Broadcaster<Bytes>,
        content_length: Option<u64>,
    ) -> (
        &'b mut broadcaster::Broadcaster<Bytes>,
        impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
    ) {
        let future = match self {
            Self::Consumer(consumer) => {
                tokio_util::either::Either::Left(consumer.consume_from_fanout(
                    download_broadcaster.subscribe(),
                    download_broadcaster.get_cancellation_token().clone(),
                    content_length,
                ))
            }
            Self::ConsumerOutput(consumer_output) => tokio_util::either::Either::Right(
                futures::future::ready(Ok(consumer_output.clone())),
            ),
        };
        (download_broadcaster, future)
    }
}

#[macro_export]
macro_rules! consumer_group {
    {
        $consumer_group_vis:vis struct $consumer_group_name:ident -> Result<$consumer_group_output_name:ident, $consumer_group_error:ident>;
        $($consumer_name:ident : $consumer_ty:ty),* $(,)?
    } => {
        #[derive(bon::Builder)]
        $consumer_group_vis struct $consumer_group_name {
            $(#[builder(into)] $consumer_name : download_fanout::consumer::ConsumerOrResolved<$consumer_ty>,)*
        }

        impl download_fanout::consumer::FanoutConsumerGroup for $consumer_group_name {
            type Output = $consumer_group_output_name;
            type Error = $consumer_group_error;
            fn _consume_from_fanout<'a, 'b>(
                &'a self,
                download_broadcaster: &'b mut stream_utils::broadcaster::Broadcaster<bytes::Bytes>,
                content_length: Option<u64>,
            ) -> (
                &'b mut stream_utils::broadcaster::Broadcaster<bytes::Bytes>,
                impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
            ) {
                $(
                    let (download_broadcaster, paste::paste! { [<$consumer_name _future>] }) = self.$consumer_name._consume_from_fanout(download_broadcaster, content_length);
                )*

                let future = async {
                    let (
                        $(paste::paste! { [<$consumer_name _result>] },)*
                    ) = tokio::join!(
                        $(paste::paste! { [<$consumer_name _future>] },)*
                    );
                    Ok(DownloadFanoutConsumersOutput {
                        $($consumer_name : paste::paste! { [<$consumer_name _result>] }?,)*
                    })
                };
                (download_broadcaster, future)
            }
        }

        #[derive(Debug)]
        $consumer_group_vis struct $consumer_group_output_name {
            $($consumer_name: <$consumer_ty as download_fanout::consumer::FanoutConsumer>::Output),*
        }
    };
}
pub use consumer_group;
