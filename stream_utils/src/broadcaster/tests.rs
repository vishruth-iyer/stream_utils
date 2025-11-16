use itertools::Itertools;

use super::Broadcaster;

#[tokio::test]
async fn test_broadcaster_all_receivers_receive_all_messages_in_order() {
    let messages = (0..5).collect_vec();
    let mut broadcaster = Broadcaster::builder()
        .buffer_size(1)
        .channel(tokio::sync::mpsc::channel)
        .build();
    let mut rx1 = broadcaster.subscribe();
    let mut rx2 = broadcaster.subscribe();
    let broadcast_future = async {
        for message in messages.clone() {
            broadcaster.broadcast(message).await.unwrap();
        }
        drop(broadcaster)
    };
    let rx1_future = async {
        let mut received_messages = Vec::new();
        let expected_messages = messages.clone();
        while let Some(message) = rx1.recv().await {
            received_messages.push(message);
        }
        drop(rx1);
        assert_eq!(
            received_messages.len(),
            expected_messages.len(),
            "received messages: {received_messages:?}, expected: {expected_messages:?}"
        );
        for (received_message, expected_message) in
            received_messages.into_iter().zip(expected_messages)
        {
            assert_eq!(
                received_message, expected_message,
                "received message: {received_message}, expected: {expected_message}"
            );
        }
    };
    let rx2_future = async {
        let mut received_messages = Vec::new();
        let expected_messages = messages.clone();
        while let Some(message) = rx2.recv().await {
            received_messages.push(message);
        }
        drop(rx2);
        assert_eq!(
            received_messages.len(),
            expected_messages.len(),
            "received messages: {received_messages:?}, expected: {expected_messages:?}"
        );
        for (received_message, expected_message) in
            received_messages.into_iter().zip(expected_messages)
        {
            assert_eq!(
                received_message, expected_message,
                "received message: {received_message}, expected: {expected_message}"
            );
        }
    };
    tokio::join!(broadcast_future, rx1_future, rx2_future);
}

/// Check that the broadcaster aborts when it receives the cancellation token signal
///
/// 1. Broadcaster sends message 0.
/// 2. Both receivers receive message 0.
/// 3. Broadcaster sends message 1.
/// 4. Both receivers receive message 1.
///    * One receiver signals via the cancellation token to abort the broadcast
/// 5. Now there are two possibilities (race condition):
///    * Either the broadcaster receives the signal before it broadcasts message 2, in which case it fails to broadcast message 2
///    * Or the broadcaster receives the signal after it broadcasts message 2, in which case it succeeds in broadcasting message 2 but fails to broadcast message 3
///
/// The test ensures that the broadcaster fails to broadcast either message 2 or message 3 and no receiver receives a message >= 3
#[tokio::test]
async fn test_broadcaster_abort_if_cancellation_token() {
    let messages = (0..5).collect_vec();
    let mut broadcaster = Broadcaster::builder()
        .buffer_size(1)
        .channel(tokio::sync::mpsc::channel)
        .build();
    let cancellation_token = broadcaster.get_cancellation_token().clone();
    let mut rx1 = broadcaster.subscribe();
    let mut rx2 = broadcaster.subscribe();
    let broadcast_future = async {
        for message in messages.clone() {
            let broadcast_result = broadcaster.broadcast(message).await;
            if broadcast_result.is_err() {
                assert!(message == 2 || message == 3, "message: {message}");
                break;
            }
        }
        drop(broadcaster);
    };
    let rx1_future = async {
        while let Some(message) = rx1.recv().await {
            if message == 1 {
                cancellation_token.cancel();
                break;
            }
        }
        drop(rx1);
    };
    let rx2_future = async {
        while let Some(message) = rx2.recv().await {
            assert!(message < 3, "message: {message}");
        }
        drop(rx2);
    };
    tokio::join!(broadcast_future, rx1_future, rx2_future);
}
