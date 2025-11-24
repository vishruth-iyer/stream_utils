use crate::{channel, fanout::consumer::CancelEgress};

pub struct StreamFanoutDriver<Source, Consumers, BroadcasterChannel, BroadcasterBufferSize, EgressSender> {
    stream_fanout: super::ReadyStreamFanout<Source, Consumers>,
    broadcaster_channel: BroadcasterChannel,
    broadcaster_buffer_size: BroadcasterBufferSize,
    egress_tx: EgressSender,
}

impl<Source, Consumers>
    StreamFanoutDriver<Source, Consumers, (), (), ()>
{
    pub(crate) fn new(
        stream_fanout: super::StreamFanout<Source, Consumers, super::Ready>,
    ) -> Self {
        Self {
            stream_fanout,
            broadcaster_channel: (),
            broadcaster_buffer_size: (),
            egress_tx: (),
        }
    }
}

impl<Source, Consumers, OldBroadcasterChannel, BroadcasterBufferSize, EgressSender>
    StreamFanoutDriver<Source, Consumers, OldBroadcasterChannel, BroadcasterBufferSize, EgressSender>
where
    Source: super::source::FanoutSource,
{
    pub fn with_broadcaster_channel<BroadcasterChannel>(
        self,
        broadcaster_channel: BroadcasterChannel,
    ) -> StreamFanoutDriver<Source, Consumers, BroadcasterChannel, BroadcasterBufferSize, EgressSender>
    where
        BroadcasterChannel: channel::Channel<Item = Source::Item>,
    {
        StreamFanoutDriver {
            stream_fanout: self.stream_fanout,
            broadcaster_channel,
            broadcaster_buffer_size: self.broadcaster_buffer_size,
            egress_tx: self.egress_tx,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, OldBroadcasterBufferSize, EgressSender>
    StreamFanoutDriver<Source, Consumers, BroadcasterChannel, OldBroadcasterBufferSize, EgressSender>
{
    pub fn with_broadcaster_buffer_size(
        self,
        broadcaster_buffer_size: usize,
    ) -> StreamFanoutDriver<Source, Consumers, BroadcasterChannel, usize, EgressSender> {
        StreamFanoutDriver { stream_fanout: self.stream_fanout, broadcaster_channel: self.broadcaster_channel, broadcaster_buffer_size, egress_tx: self.egress_tx }
    }
}

impl<Source, Consumers, BroadcasterChannel, BroadcasterBufferSize, OldEgressSender>
    StreamFanoutDriver<Source, Consumers, BroadcasterChannel, BroadcasterBufferSize, OldEgressSender>
where
    BroadcasterChannel: channel::Channel,
{
    pub fn with_egress_tx<EgressItem, EgressSender>(
        self,
        egress_tx: EgressSender,
    ) -> StreamFanoutDriver<Source, Consumers, BroadcasterChannel, BroadcasterBufferSize, EgressSender>
    where
        EgressSender: channel::sender::Sender<Item = EgressItem>,
        EgressItem: super::egress::EgressItem<BroadcasterChannel::Item>,
    {
        StreamFanoutDriver {
            stream_fanout: self.stream_fanout,
            broadcaster_channel: self.broadcaster_channel,
            broadcaster_buffer_size: self.broadcaster_buffer_size,
            egress_tx,
        }
    }
}

impl<Source, Consumers, BroadcasterChannel, EgressItem, EgressSender>
    StreamFanoutDriver<Source, Consumers, BroadcasterChannel, usize, EgressSender>
where
    Source: super::source::FanoutSource,
    Consumers: super::consumer::FanoutConsumerGroup<Item = Source::Item>,
    BroadcasterChannel: channel::Channel<Item = Source::Item>,
    BroadcasterChannel::Receiver: 'static,
    EgressItem: super::egress::EgressItem<BroadcasterChannel::Item>,
    EgressSender: super::egress::EgressSender<Item = EgressItem>,
{
    pub async fn drive(
        mut self,
    ) -> (super::UsedStreamFanout<Source, Consumers>, Result<Consumers::Output, Source::Error>) {
        let fanout_result = self.stream_fanout
            .drive_inner::<BroadcasterChannel, EgressItem, EgressSender>(
                self.broadcaster_channel,
                self.broadcaster_buffer_size,
                &self.egress_tx,
            )
            .await;
        if fanout_result.is_err() || fanout_result.as_ref().is_ok_and(Consumers::Output::cancel_egress) {
            self.egress_tx.send(EgressItem::error()).await;
        }

        (self.stream_fanout.into_used(), fanout_result)
    }
}
