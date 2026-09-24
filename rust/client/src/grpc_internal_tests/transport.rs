use super::*;

async fn assert_terminal_open_failure(code: Code, expected: TransportCode) {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica(&format!("terminal-open-{expected:?}"));
    server.service.inject(Operation::BidiWrite, code).await;
    let error = replica
        .create_append_session(HashMap::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, expected);
    assert!(!error.code.transient());
    assert_eq!(
        server.service.operation_count(Operation::BidiWrite).await,
        1
    );
}

#[tokio::test]
async fn empty_lane_group_is_rejected_without_a_bare_flush_message() {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica("empty-lane-group");
    replica.create_append_session(HashMap::new()).await.unwrap();
    assert_eq!(
        server.service.operation_count(Operation::BidiWrite).await,
        1
    );

    let error = replica.lane_send(0, &[]).await.unwrap_err();
    assert_eq!(error.code, TransportCode::Internal);
    assert_eq!(
        server.service.operation_count(Operation::BidiWrite).await,
        1
    );
}

#[tokio::test]
async fn unfinalized_create_resource_is_not_a_finish_response() {
    let server = FakeGcs::with_latency(LatencyProfile::new(393).with_operation(
        Operation::BidiFinalize,
        SimulatedLatency::fixed(Duration::from_millis(20)),
    ))
    .start()
    .await
    .unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica("unfinalized-create-resource");
    let mut token = replica.create_append_session(HashMap::new()).await.unwrap();
    let data = bytes::Bytes::from_static(b"abc");

    replica
        .lane_send(0, std::slice::from_ref(&data))
        .await
        .unwrap();
    let durable = replica.lane_durable_change(0).await.unwrap();
    assert_eq!(durable.persisted_size, data.len() as i64);
    token.persisted_size = durable.persisted_size;

    let finalized = replica
        .finalize(&mut token, data.len() as i64)
        .await
        .unwrap();
    assert!(finalized.finalized);
    assert_eq!(finalized.persisted_size, data.len() as i64);
}

#[tokio::test]
async fn terminal_grpc_statuses_are_classified_terminally() {
    assert_terminal_open_failure(Code::InvalidArgument, TransportCode::InvalidArgument).await;
    assert_terminal_open_failure(Code::Unimplemented, TransportCode::Unimplemented).await;
}

#[tokio::test]
async fn resource_exhausted_is_classified_transiently() {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica("resource-exhausted-open");
    server
        .service
        .inject(Operation::BidiWrite, Code::ResourceExhausted)
        .await;

    let error = replica
        .create_append_session(HashMap::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, TransportCode::ResourceExhausted);
    assert!(error.code.transient());
    assert_eq!(
        server.service.operation_count(Operation::BidiWrite).await,
        1
    );
}

#[tokio::test]
async fn lane_session_diagnostics_describe_the_live_session() {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica("lane-session-diagnostics");
    let idle = replica.lane_session_diagnostics();
    assert!(idle.session_id.is_none());
    assert!(idle.response_stream_open.is_none());

    replica.create_append_session(HashMap::new()).await.unwrap();
    let opened = replica.lane_session_diagnostics();
    assert!(opened.session_id.is_some());
    assert_eq!(opened.response_stream_open, Some(true));
}

#[tokio::test]
async fn one_shot_writes_larger_than_the_inbound_cap_round_trip() {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/zone-0", None)
            .await
            .unwrap();
    let replica = factory.replica("one-shot-over-cap");
    let cap = chorus_fake_gcs::INBOUND_MESSAGE_CAP_BYTES;
    let appended: Vec<u8> = (0..2 * cap + 11).map(|index| index as u8).collect();

    let created = replica.create_appendable(HashMap::new()).await.unwrap();
    let token = replica.takeover(&created).await.unwrap();
    let persisted = replica.append(&token, 0, appended.clone()).await.unwrap();
    assert_eq!(persisted, appended.len() as i64);
    let observed = replica.snapshot().await.unwrap();
    assert_eq!(observed.bytes, appended);

    // A fresh handle, as repair uses: `takeover` left a live session on the
    // first one, and `finalize` would finish that session.
    let replica = factory.replica("one-shot-over-cap");
    let replacement = bytes::Bytes::from(vec![0x5a; cap + 7]);
    let metadata = HashMap::from([("chorus.format".to_string(), "1".to_string())]);
    let mut token = replica
        .replace_appendable(&observed, replacement.clone(), metadata.clone())
        .await
        .unwrap();
    assert_eq!(token.persisted_size, replacement.len() as i64);
    let finalized = replica
        .finalize(&mut token, replacement.len() as i64)
        .await
        .unwrap();
    assert_eq!(finalized.crc32c, Some(crc32c::crc32c(&replacement)));
    let replaced = replica.snapshot().await.unwrap();
    assert!(replaced.finalized);
    assert_eq!(replaced.bytes, replacement);
    assert_eq!(replaced.metadata, metadata);
}
