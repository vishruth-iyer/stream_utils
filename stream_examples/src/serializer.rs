use itertools::Itertools;
use tokio::io::AsyncReadExt;

pub(super) async fn main() {
    run().await;
    let _ = tokio::task::LocalSet::new()
        .run_until(async { tokio::task::spawn_local(run()).await }) // make sure it works with spawn_local trait bounds
        .await;
    let _ = tokio::task::spawn(run()).await; // make sure it works with spawn trait bounds
}

async fn run() {
    const SERIALIZED_NAME: &str = "numbers";
    const ITEMS: std::ops::Range<i32> = 0..10;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let sender_future = async move {
        for i in ITEMS.clone() {
            let _ = tx.send(i).await;
        }
    };
    let (output_tx, mut output_rx) = tokio::io::duplex(1);
    let serialize_thread = tokio::task::spawn_blocking(move || {
        let receiver_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let serialize_stream =
            stream_utils::serializer::StreamToSerialize::new(SERIALIZED_NAME, receiver_stream);
        let mut serializer =
            serde_json::Serializer::new(tokio_util::io::SyncIoBridge::new(output_tx));
        serialize_stream.serialize(&mut serializer)
    });
    let mut buf = Vec::with_capacity(33);
    let (_, serialize_result, read_result) = tokio::join!(
        sender_future,
        serialize_thread,
        output_rx.read_to_end(&mut buf)
    );
    serialize_result.unwrap().unwrap();
    read_result.unwrap();
    let result = ITEMS.clone().join(",");
    assert_eq!(
        format!("{{\"{SERIALIZED_NAME}\":[{result}]}}"),
        String::from_utf8_lossy(&buf)
    );
}
