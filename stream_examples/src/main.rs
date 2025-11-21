mod download_fanout;
mod serializer;

#[tokio::main]
async fn main() {
    download_fanout::main().await;
    serializer::main().await;
}
