mod fanout;
mod serializer;

#[tokio::main]
async fn main() {
    fanout::main().await;
    serializer::main().await;
}
