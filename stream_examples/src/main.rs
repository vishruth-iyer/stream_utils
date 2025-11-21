mod download_fanout;

#[tokio::main]
async fn main() {
    download_fanout::main().await;
}
