use serde::Serialize;

mod private;

pub struct StreamToSerialize<'a, Stream> {
    name: &'a str,
    stream: Stream,
    length: Option<usize>,
}

impl<'a, Stream> StreamToSerialize<'a, Stream> {
    pub fn new(name: &'a str, stream: Stream) -> Self {
        Self {
            name,
            stream,
            length: None,
        }
    }

    pub fn new_with_length(name: &'a str, stream: Stream, length: usize) -> Self {
        Self {
            name,
            stream,
            length: Some(length),
        }
    }
}

impl<'a, Stream> StreamToSerialize<'a, Stream>
where
    Stream: futures::Stream + Unpin,
    Stream::Item: serde::Serialize,
{
    pub fn serialize<S>(self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        private::StreamSerializerWithName::from(self).serialize(serializer)
    }
}
