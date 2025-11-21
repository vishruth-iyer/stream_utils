use futures::StreamExt;
use serde::ser::{SerializeMap, SerializeSeq};

pub(super) struct StreamSerializerWithName<'a, Stream> {
    name: &'a str,
    stream_serializer: StreamSerializer<Stream>,
}

impl<'a, Stream> From<super::StreamToSerialize<'a, Stream>> for StreamSerializerWithName<'a, Stream> {
    fn from(value: super::StreamToSerialize<'a, Stream>) -> Self {
        Self {
            name: value.name,
            stream_serializer: StreamSerializer::new(value.stream, value.length),
        }
    }
}

impl<'a, Stream> serde::Serialize for StreamSerializerWithName<'a, Stream>
where
    Stream: futures::Stream + Unpin,
    Stream::Item: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.name, &self.stream_serializer)?;
        map.end()
    }
}

struct StreamSerializer<Stream> {
    stream: std::cell::RefCell<Stream>,
    length: Option<usize>,
}

impl<Stream> StreamSerializer<Stream> {
    fn new(stream: Stream, length: Option<usize>) -> Self {
        Self {
            stream: std::cell::RefCell::new(stream),
            length,
        }
    }
}

impl<Stream> serde::Serialize for StreamSerializer<Stream>
where
    Stream: futures::Stream + Unpin,
    Stream::Item: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // there should never be a conflicting borrow as long as the apis above are used
        let mut stream = self
            .stream
            .try_borrow_mut()
            .map_err(serde::ser::Error::custom)?;
        let mut seq = serializer.serialize_seq(self.length)?;
        let mut items_serialized = 0;
        while let Some(item) = futures::executor::block_on(stream.next()) {
            seq.serialize_element(&item)?;
            items_serialized += 1;
            if self.length.is_some_and(|length| items_serialized >= length) {
                break;
            }
        }
        seq.end()
    }
}
