use super::*;
use slatedb::wal::WalReader;

fn assert_unsupported(error: WalError) {
    let WalError::InternalError(error) = error else {
        panic!("expected unsupported error")
    };
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn readonly_operations_are_unsupported_without_touching_storage() {
    let (servers, volume) = volume().await;
    let wal = ChorusWal::new(volume);
    assert_unsupported(wal.last_wal_file_id(0).await.unwrap_err());
    for range in [(1..).into(), (1..3).into()] {
        match wal.iterator(range).await {
            Err(error) => assert_unsupported(error),
            Ok(_) => panic!("readonly iterator unexpectedly supported"),
        }
    }
    for (zone, server) in servers.iter().enumerate() {
        assert!(server
            .service
            .observe_prefix(&format!("projects/_/buckets/zone-{zone}"), "slatedb-test")
            .await
            .is_empty());
    }
}
