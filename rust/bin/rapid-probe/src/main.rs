//! Live-bucket verification of the GCS provider behavior characterized by the
//! Chorus whitepaper. The load-bearing fencing decision is **D1**: a fresh,
//! handle-free, metageneration-guarded append open revokes the prior writer's
//! stream. The suite also measures related append, read, listing, finalization,
//! and metadata behavior.
//!
//! The proto (`google/storage/v2/storage.proto`) tells us two things statically:
//!   * `AppendObjectSpec.if_metageneration_match` exists and `append_object_spec`
//!     is in the `BidiWriteObjectRequest.first_message` oneof, so a takeover open
//!     can be guarded by the observed object's metageneration; and
//!   * "metageneration preconditions are only checked if `write_handle` is empty".
//!
//! What the proto CANNOT tell us is whether opening a fresh append stream
//! **revokes a prior writer's open stream** (takeover). This binary establishes
//! that load-bearing behavior and the surrounding provider characteristics
//! empirically against a real Rapid zonal bucket.
//!
//! Probes (each uses unique scratch object names and attempts cleanup):
//!   T1  precondition enforced on a fresh open (no write_handle):
//!         wrong if_metageneration_match -> FAILED_PRECONDITION; correct -> OK.
//!   T2  a metadata CAS bumps metageneration and fences subsequent guarded opens:
//!         stale if_metageneration_match -> FAILED_PRECONDITION; current -> OK.
//!   T3  TAKEOVER: a second fresh open revokes a held-open prior stream:
//!         after S2 opens+appends, S1's continued append must FAIL.   (D1 valid)
//!   T4  a metadata CAS does NOT fence an already-open stream.
//!   T5  finalization rejects a subsequent append open.
//!   T6  newly created segment-like objects are immediately visible to listing.
//!   T7  reports open-object read/size visibility and verifies the finalized read.
//!   T8  reports whether repeated flushed appends encounter throttling.
//!   T9  measures flush latency and one-final-flush group durability.
//!   T10 reports outcomes for a fixed matrix of write-message sizes.
//!   T11 metadata CAS preserves an already-open stream under repeated writes.
//!   T12 an appendable create stream accepts continuations and finalization.
//!   TODO(T13): verify that finalize and immediate GetObject responses for a
//!     finalized Rapid APPENDABLE object populate Object.checksums.crc32c.
//!
//! Expected rejections and failures print the raw observed gRPC status code and
//! message; successful characterization results print their sizes or timings.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use chorus_client::{BearerAuth, GrpcReplicaFactory, TransportCode};
use clap::Parser;
use google_cloud_auth::credentials::Builder as AdcCredentialsBuilder;
use googleapis_tonic_google_storage_v2::google::storage::v2::{
    bidi_write_object_request::{Data, FirstMessage},
    bidi_write_object_response::WriteStatus,
    storage_client::StorageClient,
    AppendObjectSpec, BidiWriteObjectRequest, BidiWriteObjectResponse, ChecksummedData,
    DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, Object, ReadObjectRequest,
    UpdateObjectRequest, WriteObjectSpec,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status, Streaming};

const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

#[derive(Parser, Debug)]
#[command(
    name = "rapid-probe",
    about = "Probe live GCS Rapid append, fencing, visibility, and finalization behavior"
)]
struct Args {
    /// gRPC endpoint. For a zonal Rapid bucket use its regional/zonal endpoint.
    #[arg(long, default_value = "https://storage.googleapis.com")]
    endpoint: String,

    /// Target bucket you may write to and delete from. Accepts a bare name
    /// (normalized to `projects/_/buckets/<name>`) or a full resource path.
    #[arg(long)]
    bucket: String,

    /// Object-name prefix. A unique run id and probe tag are appended.
    #[arg(long, default_value = "rapid-probe")]
    object_prefix: String,

    /// Static OAuth bearer token. If omitted, Application Default Credentials.
    #[arg(long)]
    bearer_token: Option<String>,

    /// Keep probe objects instead of deleting them at the end.
    #[arg(long)]
    keep: bool,

    /// Run only the production-client generation-zero current-object probe.
    #[arg(long)]
    generation_zero_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let token = match &args.bearer_token {
        Some(token) => token.clone(),
        None => adc_token().await.context("load ADC access token")?,
    };
    let auth: MetadataValue<Ascii> = MetadataValue::try_from(format!("Bearer {token}"))
        .context("bearer token is not valid gRPC metadata")?;

    let mut builder = Endpoint::from_shared(args.endpoint.clone())?;
    if args.endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    let channel = builder.connect().await.context("connect to endpoint")?;
    let client = StorageClient::new(channel);

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // GCS v2 requires the bucket as a resource path; a bare name parses to an
    // empty bucket server-side ("Bucket '' not found").
    let bucket = if args.bucket.starts_with("projects/") {
        args.bucket.clone()
    } else {
        format!("projects/_/buckets/{}", args.bucket)
    };
    let ctx = Ctx {
        client,
        auth,
        bucket: bucket.clone(),
        routing_token: Mutex::new(None),
        keep: args.keep,
    };

    if args.generation_zero_only {
        let factory = GrpcReplicaFactory::connect_with_auth(
            0,
            &args.endpoint,
            bucket.clone(),
            BearerAuth::static_token(token),
        )
        .await
        .context("connect production Chorus gRPC transport")?;
        let present_object = format!("{}-{run_id}-generation-zero-present", args.object_prefix);
        let absent_object = format!("{}-{run_id}-generation-zero-absent", args.object_prefix);
        let result = chorus_client::probe_support::probe_generation_zero_takeover(
            &factory,
            &present_object,
            &absent_object,
            Bytes::from_static(b"chorus-generation-zero-takeover-probe"),
        )
        .await
        .context("generation-zero takeover probe failed")?;
        let append_pass = match &result.append {
            Ok(persisted_size) => {
                println!(
                    "STEP 1(a) append: N={} bytes; flush persisted_size={persisted_size}",
                    result.expected_size
                );
                *persisted_size == result.expected_size
            }
            Err(error) => {
                println!(
                    "STEP 1(a) append: error_code={} (TransportCode::{:?}) message={:?}",
                    transport_code_name(error.code),
                    error.code,
                    error.message
                );
                false
            }
        };
        let takeover_pass = match &result.takeover {
            Some(Ok(observation)) => {
                println!(
                    "STEP 1(a) generation=0 takeover: object_resource_size={:?}; \
                     persisted_size={}",
                    observation.resource_size, observation.persisted_size
                );
                observation.resource_size == Some(result.expected_size)
                    && observation.persisted_size == result.expected_size
            }
            Some(Err(error)) => {
                println!(
                    "STEP 1(a) generation=0 takeover: error_code={} \
                     (TransportCode::{:?}) message={:?}; object_resource_size=None",
                    transport_code_name(error.code),
                    error.code,
                    error.message
                );
                false
            }
            None => {
                println!("STEP 1(a) generation=0 takeover: not attempted after append failure");
                false
            }
        };
        let absent_pass = match &result.absent {
            Ok(observation) => {
                println!(
                    "STEP 1(b) never-created generation=0 takeover: unexpectedly succeeded; \
                     object_resource_size={:?}; persisted_size={}",
                    observation.resource_size, observation.persisted_size
                );
                false
            }
            Err(error) => {
                println!(
                    "STEP 1(b) never-created generation=0 takeover: error_code={} \
                     (TransportCode::{:?}) message={:?}",
                    transport_code_name(error.code),
                    error.code,
                    error.message
                );
                error.code == TransportCode::NotFound
            }
        };
        let cleanup_pass = match &result.cleanup {
            Ok(()) => {
                println!("STEP 1(c) cleanup: deleted object {present_object}");
                true
            }
            Err(error) => {
                println!(
                    "STEP 1(c) cleanup: error_code={} (TransportCode::{:?}) message={:?}",
                    transport_code_name(error.code),
                    error.code,
                    error.message
                );
                false
            }
        };
        if append_pass && takeover_pass && absent_pass && cleanup_pass {
            println!("GENERATION-ZERO TAKEOVER PROBE PASSED");
            return Ok(());
        }
        anyhow::bail!("GENERATION-ZERO TAKEOVER PROBE FAILED");
    }

    println!("rapid-probe against {} bucket={}", args.endpoint, bucket);
    println!("run id: {run_id}\n");

    let mut all_pass = true;
    all_pass &= report(
        "T1 precondition-on-fresh-open",
        probe_t1(&ctx, run_id).await,
    );
    all_pass &= report(
        "T2 metadata-CAS-fences-new-open",
        probe_t2(&ctx, run_id).await,
    );
    all_pass &= report(
        "T3 takeover-revokes-prior-stream",
        probe_t3(&ctx, run_id).await,
    );
    all_pass &= report(
        "T4 metadata-CAS-does-not-fence-open-stream",
        probe_t4(&ctx, run_id).await,
    );
    all_pass &= report(
        "T5 finalize-rejects-further-appends",
        probe_t5(&ctx, run_id).await,
    );
    all_pass &= report(
        "T6 strongly-consistent-listing",
        probe_t6(&ctx, run_id).await,
    );
    all_pass &= report(
        "T7 reads-of-open-appendable-objects",
        probe_t7(&ctx, run_id).await,
    );
    all_pass &= report("T8 per-record-flush-rate", probe_t8(&ctx, run_id).await);
    all_pass &= report("T9 append-latency-anatomy", probe_t9(&ctx, run_id).await);
    all_pass &= report("T10 max-write-message-size", probe_t10(&ctx, run_id).await);
    all_pass &= report(
        "T11 metadata-CAS-preserves-open-session",
        probe_t11(&ctx, run_id).await,
    );
    all_pass &= report("T12 create-stream-lifetime", probe_t12(&ctx, run_id).await);

    println!(
        "\n{}",
        if all_pass {
            "ALL PROBES MATCHED EXPECTATIONS"
        } else {
            "SOME PROBES DID NOT MATCH — see above"
        }
    );
    if all_pass {
        Ok(())
    } else {
        std::process::exit(1);
    }
}

struct Ctx {
    client: StorageClient<Channel>,
    auth: MetadataValue<Ascii>,
    bucket: String,
    /// Zonal write routing token, learned from the first BidiWriteObjectRedirectedError
    /// and replayed on every later RPC. Reuses the production client's extractor.
    routing_token: Mutex<Option<String>>,
    keep: bool,
}

const MAX_REDIRECTS: u32 = 5;
/// Upper bound on how long to wait for a single BidiWriteObject response before
/// treating the held-open stream as unresponsive (used by T3/T4).
const READ_TIMEOUT_SECS: u64 = 20;

impl Ctx {
    /// Wrap a message with the auth and x-goog-request-params routing headers
    /// that every GCS v2 RPC requires (carrying the routing token once known).
    fn request<T>(&self, message: T) -> Request<T> {
        let value = match self.routing_token.lock().unwrap().as_deref() {
            Some(token) => format!("bucket={}&routing_token={}", self.bucket, token),
            None => format!("bucket={}", self.bucket),
        };
        let params: MetadataValue<Ascii> =
            MetadataValue::try_from(value).expect("request params are valid gRPC metadata");
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.auth.clone());
        request
            .metadata_mut()
            .insert("x-goog-request-params", params);
        request
    }

    /// If `status` is a zonal write redirect, cache its routing token (via the
    /// production client's extractor) and report `true` so the caller retries.
    fn capture_redirect(&self, status: &Status) -> bool {
        match chorus_client::probe_support::redirect_routing_token(status) {
            Some(token) => {
                *self.routing_token.lock().unwrap() = Some(token);
                true
            }
            None => false,
        }
    }
}

struct ProbeResult {
    pass: bool,
    detail: String,
}

fn ok(detail: impl Into<String>) -> Result<ProbeResult> {
    Ok(ProbeResult {
        pass: true,
        detail: detail.into(),
    })
}
fn bad(detail: impl Into<String>) -> Result<ProbeResult> {
    Ok(ProbeResult {
        pass: false,
        detail: detail.into(),
    })
}

fn report(name: &str, result: Result<ProbeResult>) -> bool {
    match result {
        Ok(ProbeResult { pass, detail }) => {
            println!(
                "[{}] {name}\n      {detail}",
                if pass { "PASS" } else { "FAIL" }
            );
            pass
        }
        Err(error) => {
            println!("[ERR ] {name}\n      probe could not run: {error:#}");
            false
        }
    }
}

fn code_of(status: &Status) -> String {
    format!("{:?} ({})", status.code(), status.message())
}

fn transport_code_name(code: TransportCode) -> &'static str {
    match code {
        TransportCode::NotFound => "NOT_FOUND",
        TransportCode::AlreadyExists => "ALREADY_EXISTS",
        TransportCode::InvalidArgument => "INVALID_ARGUMENT",
        TransportCode::FailedPrecondition => "FAILED_PRECONDITION",
        TransportCode::Aborted => "ABORTED",
        TransportCode::OutOfRange => "OUT_OF_RANGE",
        TransportCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
        TransportCode::Unimplemented => "UNIMPLEMENTED",
        TransportCode::DataLoss => "DATA_LOSS",
        TransportCode::Ambiguous => "AMBIGUOUS",
        TransportCode::Unauthenticated => "UNAUTHENTICATED",
        TransportCode::PermissionDenied => "PERMISSION_DENIED",
        TransportCode::Unavailable => "UNAVAILABLE",
        TransportCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
        TransportCode::Internal => "INTERNAL",
    }
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

async fn probe_t1(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t1");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;

    let wrong = metageneration + 9_999;
    let bad_open = append_once(ctx, &object, generation, Some(wrong), size, b"AAAAAAAA").await;
    let good_open = append_once(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        b"BBBBBBBB",
    )
    .await;

    cleanup(ctx, &object, generation).await;

    match (&bad_open, &good_open) {
        (Err(s), Ok(_)) if s.code() == tonic::Code::FailedPrecondition => ok(format!(
            "wrong if_metageneration_match -> {}; correct -> Ok",
            code_of(s)
        )),
        (Err(s), Ok(_)) => bad(format!(
            "wrong metagen rejected but with unexpected code {}; correct -> Ok",
            code_of(s)
        )),
        (Ok(sz), _) => bad(format!(
            "wrong if_metageneration_match was ACCEPTED (persisted {sz}); precondition not enforced on open"
        )),
        (_, Err(s)) => bad(format!("correct if_metageneration_match was rejected: {}", code_of(s))),
    }
}

async fn probe_t2(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t2");
    create_appendable(ctx, &object).await?;
    let (generation, m0, size) = stat(ctx, &object).await?;

    // A metadata CAS bumps metageneration m0 -> m1.
    bump_metageneration(ctx, &object, generation, m0).await?;
    let (_, m1, size2) = stat(ctx, &object).await?;

    let stale = append_once(ctx, &object, generation, Some(m0), size2, b"CCCCCCCC").await;
    let current = append_once(ctx, &object, generation, Some(m1), size2, b"DDDDDDDD").await;

    cleanup(ctx, &object, generation).await;

    if m1 <= m0 {
        return bad(format!(
            "UpdateObject did not bump metageneration (m0={m0}, m1={m1})"
        ));
    }
    let _ = size;
    match (&stale, &current) {
        (Err(s), Ok(_)) if s.code() == tonic::Code::FailedPrecondition => ok(format!(
            "metageneration {m0}->{m1}; stale open -> {}; current open -> Ok",
            code_of(s)
        )),
        (Err(s), Ok(_)) => bad(format!(
            "stale open rejected with unexpected code {}",
            code_of(s)
        )),
        (Ok(sz), _) => bad(format!(
            "stale if_metageneration_match ACCEPTED (persisted {sz}) after CAS"
        )),
        (_, Err(s)) => bad(format!(
            "current if_metageneration_match rejected: {}",
            code_of(s)
        )),
    }
}

async fn probe_t3(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t3");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;

    // S1: held-open writer; its persisted-size response is the authoritative tail.
    let (mut s1, s1_first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        b"11111111",
    )
    .await?;
    let s1_tail = match s1_first {
        Ok(sz) => sz,
        Err(s) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!(
                "S1 could not establish its stream: {}",
                code_of(&s)
            ));
        }
    };

    // S2: a fresh open by the "new writer", appending at S1's tail. This is the
    // takeover. Its persisted size becomes the new object tail.
    let s2 = append_once(
        ctx,
        &object,
        generation,
        Some(metageneration),
        s1_tail,
        b"22222222",
    )
    .await;

    // The decisive split-brain test: S1 continues on its already-open handle,
    // appending at the tail ITS OWN handle believes in (s1_tail). If GCS lets
    // this through, the deposed writer forks the object -> takeover is not a
    // fence. If it is rejected, the prior writer can no longer persist.
    let s1_after = send_continuation(&mut s1, s1_tail, b"33333333").await;
    drop(s1);
    cleanup(ctx, &object, generation).await;

    let trace = format!(
        "[s1_tail={s1_tail} s2={:?} s1_retry_offset={s1_tail}]",
        s2.as_ref().map(|v| *v).map_err(|e| e.code())
    );
    match (&s2, &s1_after) {
        (Ok(_), Err(s)) => ok(format!(
            "S2 takeover succeeded; S1's append at its own believed tail was rejected -> {} (deposed writer cannot persist) {trace}",
            code_of(s)
        )),
        (Ok(_), Ok(sz)) => bad(format!(
            "SPLIT-BRAIN: after S2 takeover, S1 STILL persisted to {sz} at its stale tail — GCS did NOT fence the prior stream; D1's takeover fence is INVALID {trace}"
        )),
        (Err(s), _) => bad(format!(
            "S2 takeover open itself failed -> {} (takeover may be error+retry, not silent revoke) {trace}",
            code_of(s)
        )),
    }
}

async fn probe_t4(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t4");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;

    let (mut s1, s1_first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        b"11111111",
    )
    .await?;
    let s1_tail = match s1_first {
        Ok(sz) => sz,
        Err(s) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!(
                "S1 could not establish its stream: {}",
                code_of(&s)
            ));
        }
    };

    // Non-fencing control: a metadata CAS that bumps metageneration.
    bump_metageneration(ctx, &object, generation, metageneration).await?;

    // S1 continues on its already-open handle at its own tail; the bump should
    // NOT have fenced it.
    let s1_after = send_continuation(&mut s1, s1_tail, b"22222222").await;
    drop(s1);
    cleanup(ctx, &object, generation).await;

    let trace = format!("[s1_tail={s1_tail}]");
    match &s1_after {
        Ok(sz) => ok(format!(
            "metageneration bumped under an open stream; S1 still persisted to {sz} — metadata CAS does NOT fence an open handle (hence takeover is required) {trace}"
        )),
        Err(s) => bad(format!(
            "S1 was unexpectedly fenced by the metadata CAS -> {} {trace}",
            code_of(s)
        )),
    }
}

async fn probe_t5(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t5");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let tail = append_once(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        b"finalize",
    )
    .await
    .map_err(|status| anyhow::anyhow!("append before finalize: {}", code_of(&status)))?;
    finalize_once(ctx, &object, generation, metageneration, tail)
        .await
        .map_err(|status| anyhow::anyhow!("finalize: {}", code_of(&status)))?;
    let (_, final_metageneration, final_size) = stat(ctx, &object).await?;
    let append = append_once(
        ctx,
        &object,
        generation,
        Some(final_metageneration),
        final_size,
        b"rejected",
    )
    .await;
    cleanup(ctx, &object, generation).await;
    match append {
        Err(status) if status.code() == tonic::Code::FailedPrecondition => ok(format!(
            "append after finalization rejected -> {}",
            code_of(&status)
        )),
        Err(status) => bad(format!(
            "append after finalization rejected with unexpected status {}",
            code_of(&status)
        )),
        Ok(size) => bad(format!(
            "append after finalization unexpectedly persisted through {size}"
        )),
    }
}

async fn probe_t6(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let prefix = format!("{run_id}-t6-segments/");
    let first = format!("{prefix}00000000000000000009");
    let second = format!("{prefix}00000000000000000001");
    create_appendable(ctx, &first).await?;
    create_appendable(ctx, &second).await?;
    let first_generation = get_object(ctx, &first).await?.generation;
    let second_generation = get_object(ctx, &second).await?.generation;
    let response = ctx
        .client
        .clone()
        .list_objects(ctx.request(ListObjectsRequest {
            parent: ctx.bucket.clone(),
            prefix: prefix.clone(),
            page_size: 100,
            ..Default::default()
        }))
        .await
        .context("ListObjects immediately after create")?
        .into_inner();
    let names: Vec<_> = response
        .objects
        .into_iter()
        .map(|object| object.name)
        .collect();
    cleanup(ctx, &first, first_generation).await;
    cleanup(ctx, &second, second_generation).await;
    if names.iter().any(|name| name == &first) && names.iter().any(|name| name == &second) {
        ok(format!(
            "both newly created objects were immediately listed under {prefix}"
        ))
    } else {
        bad(format!("listing omitted a newly created object: {names:?}"))
    }
}

// ---------------------------------------------------------------------------
// GCS helpers. Redirect decoding intentionally reuses chorus-client's public
// extractor so this probe also protects that public API contract.
// ---------------------------------------------------------------------------

async fn adc_token() -> Result<String> {
    let credentials = AdcCredentialsBuilder::default()
        .with_scopes([SCOPE])
        .build_access_token_credentials()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let token = credentials
        .access_token()
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(token.token)
}

fn persisted(response: &BidiWriteObjectResponse) -> Option<i64> {
    match &response.write_status {
        Some(WriteStatus::PersistedSize(size)) => Some(*size),
        Some(WriteStatus::Resource(object)) => Some(object.size),
        None => None,
    }
}

async fn create_appendable(ctx: &Ctx, object: &str) -> Result<()> {
    let resource = Object {
        bucket: ctx.bucket.clone(),
        name: object.to_string(),
        content_type: "application/vnd.chorus.verify".into(),
        ..Default::default()
    };
    let template = BidiWriteObjectRequest {
        first_message: Some(FirstMessage::WriteObjectSpec(WriteObjectSpec {
            resource: Some(resource),
            if_generation_match: Some(0),
            appendable: Some(true),
            ..Default::default()
        })),
        write_offset: 0,
        flush: true,
        state_lookup: true,
        ..Default::default()
    };
    // The first write to a zonal bucket is redirected; capture the routing token
    // and retry. This warms ctx.routing_token so every later RPC carries it.
    for attempt in 0..=MAX_REDIRECTS {
        let open = ctx
            .client
            .clone()
            .bidi_write_object(ctx.request(tokio_stream::iter([template.clone()])))
            .await;
        let mut stream = match open {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if attempt < MAX_REDIRECTS && ctx.capture_redirect(&status) {
                    continue;
                }
                return Err(status).context("create appendable object");
            }
        };
        let mut received = false;
        loop {
            match stream.message().await {
                Ok(Some(_)) => received = true,
                Ok(None) => return Ok(()),
                Err(status) => {
                    if !received && attempt < MAX_REDIRECTS && ctx.capture_redirect(&status) {
                        break;
                    }
                    return Err(status).context("create appendable object");
                }
            }
        }
    }
    anyhow::bail!("create appendable object: exhausted redirects")
}

/// T8: how many per-record flushed appends does ONE held-open session allow?
/// Distinguishes "every flush is a rate-limited object mutation" from "only
/// opens are mutations". Sends 100 small appends on one stream, each with
/// flush+state_lookup, and reports where (if anywhere) the service throttles.
/// T9: append latency anatomy on one held session. Measures (a) sequential
/// per-flush ack RTT percentiles, (b) ack pacing of a pipelined burst (are
/// server-side flushes serialized?), and (c) the same burst with ONE flush on
/// the last message (group commit), which is the production client's send
/// pattern.
/// T10: which fixed single-message sizes does the service accept?
/// The current storage.proto documents MaxReadChunkBytes = 2 MiB but no
/// write-side constant. Sends one flushed message per size and reports what
/// the service accepts.
/// T11: supplementary metadata behavior. Stamp segment-like custom metadata
/// via CAS while an append session is held open, then verify the same session
/// still appends and flushes afterward. Chorus stamps segments after
/// finalization, so this is provider characterization rather than a protocol
/// fence assumption.
/// T12: keep the conditional-create stream itself, append through a
/// continuation with no first message, then finalize through another
/// continuation and require the authoritative object resource.
async fn probe_t12(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t12");
    let resource = Object {
        bucket: ctx.bucket.clone(),
        name: object.clone(),
        content_type: "application/vnd.chorus.verify".into(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel::<BidiWriteObjectRequest>(8);
    tx.send(BidiWriteObjectRequest {
        first_message: Some(FirstMessage::WriteObjectSpec(WriteObjectSpec {
            resource: Some(resource),
            if_generation_match: Some(0),
            appendable: Some(true),
            ..Default::default()
        })),
        write_offset: 0,
        flush: true,
        state_lookup: true,
        ..Default::default()
    })
    .await
    .map_err(|_| anyhow::anyhow!("create stream request channel closed"))?;
    let mut resp = ctx
        .client
        .clone()
        .bidi_write_object(ctx.request(ReceiverStream::new(rx)))
        .await
        .context("open appendable create stream")?
        .into_inner();
    let opening = tokio::time::timeout(
        std::time::Duration::from_secs(READ_TIMEOUT_SECS),
        resp.message(),
    )
    .await
    .context("appendable create response timed out")??;
    if opening.and_then(|response| persisted(&response)) != Some(0) {
        return bad("appendable create did not report an empty durable tail");
    }

    let mut held = HeldStream { tx, resp };
    let payload = b"create-stream-continuation";
    let tail = match send_continuation(&mut held, 0, payload).await {
        Ok(tail) => tail,
        Err(status) => return bad(format!("create continuation failed: {}", code_of(&status))),
    };
    held.tx
        .send(BidiWriteObjectRequest {
            first_message: None,
            write_offset: tail,
            finish_write: true,
            ..Default::default()
        })
        .await
        .map_err(|_| anyhow::anyhow!("create stream closed before finalization"))?;
    let finalized = loop {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(READ_TIMEOUT_SECS),
            held.resp.message(),
        )
        .await
        .context("create-stream finalization response timed out")??;
        match response {
            Some(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(resource)),
                ..
            }) => break resource,
            Some(_) => {}
            None => return bad("create stream closed before returning a finalized resource"),
        }
    };
    drop(held);
    let generation = finalized.generation;
    let valid = finalized.finalize_time.is_some() && finalized.size == tail;
    cleanup(ctx, &object, generation).await;
    if !valid {
        return bad(format!(
            "finalized resource reported size {} at expected tail {tail}",
            finalized.size
        ));
    }
    ok(format!(
        "one create RPC appended and finalized {tail} bytes through continuations"
    ))
}

async fn probe_t11(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t11");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let payload = vec![0x42u8; 4096];
    let (mut held, first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        &payload,
    )
    .await?;
    let mut tail = match first {
        Ok(size) => size,
        Err(status) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!("opening append failed: {}", code_of(&status)));
        }
    };
    let mut stamps = Vec::new();
    for round in 0..2u32 {
        // Representative segment metadata CAS while a stream remains open.
        let (_, metageneration_now, _) = stat(ctx, &object).await?;
        let mut metadata = HashMap::new();
        metadata.insert("chorus.format".to_string(), "1".to_string());
        metadata.insert("chorus.base".to_string(), format!("{}", 1000 + round));
        let update = UpdateObjectRequest {
            object: Some(Object {
                bucket: ctx.bucket.clone(),
                name: object.clone(),
                generation,
                metadata,
                ..Default::default()
            }),
            if_generation_match: Some(generation),
            if_metageneration_match: Some(metageneration_now),
            update_mask: Some(prost_types::FieldMask {
                paths: vec!["metadata".into()],
            }),
            ..Default::default()
        };
        if let Err(status) = ctx.client.clone().update_object(ctx.request(update)).await {
            cleanup(ctx, &object, generation).await;
            return bad(format!(
                "metadata stamp {round} failed: {}",
                code_of(&status)
            ));
        }
        // the held session must still append + flush after the stamp
        match send_continuation(&mut held, tail, &payload).await {
            Ok(new_tail) => {
                stamps.push(format!(
                    "stamp {round}: session persisted through {new_tail} after CAS"
                ));
                tail = new_tail;
            }
            Err(status) => {
                cleanup(ctx, &object, generation).await;
                return bad(format!(
                    "session died after metadata stamp {round}: {} {}",
                    code_of(&status),
                    status.message()
                ));
            }
        }
    }
    drop(held);
    cleanup(ctx, &object, generation).await;
    ok(stamps.join("; "))
}

async fn probe_t10(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t10");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let opener = vec![0x5au8; 8];
    let (mut held, first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        &opener,
    )
    .await?;
    let mut tail = match first {
        Ok(size) => size,
        Err(status) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!("opening append failed: {}", code_of(&status)));
        }
    };
    let sizes: &[(usize, &str)] = &[
        (262_144, "256KiB"),
        (1_048_576, "1MiB"),
        (2_097_152, "2MiB"),
        (2_097_153, "2MiB+1"),
        (4_194_304, "4MiB"),
    ];
    let mut findings = Vec::new();
    for (bytes, label) in sizes {
        let payload = vec![0xc3u8; *bytes];
        match send_continuation(&mut held, tail, &payload).await {
            Ok(new_tail) => {
                findings.push(format!("{label}: OK (persisted {new_tail})"));
                tail = new_tail;
            }
            Err(status) => {
                findings.push(format!(
                    "{label}: {} {}",
                    code_of(&status),
                    status.message()
                ));
                // the stream is dead after a rejection; reopen to continue
                let (generation_now, metageneration_now, size_now) = stat(ctx, &object).await?;
                let _ = generation_now;
                match open_held(
                    ctx,
                    &object,
                    generation,
                    Some(metageneration_now),
                    size_now,
                    &opener,
                )
                .await
                {
                    Ok((reopened, Ok(new_tail))) => {
                        held = reopened;
                        tail = new_tail;
                    }
                    _ => {
                        findings.push("(could not reopen after rejection)".to_string());
                        break;
                    }
                }
            }
        }
    }
    drop(held);
    cleanup(ctx, &object, generation).await;
    ok(findings.join("; "))
}

async fn probe_t9(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t9");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let payload = vec![0xa5u8; 4096];
    let (mut held, first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        &payload,
    )
    .await?;
    let mut tail = match first {
        Ok(size) => size,
        Err(status) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!("first append failed: {}", code_of(&status)));
        }
    };

    // (a) sequential: send, await ack, repeat
    let mut rtts = Vec::with_capacity(200);
    for _ in 0..200 {
        let begun = std::time::Instant::now();
        match send_continuation(&mut held, tail, &payload).await {
            Ok(size) => {
                rtts.push(begun.elapsed().as_micros() as u64);
                tail = size;
            }
            Err(status) => {
                cleanup(ctx, &object, generation).await;
                return bad(format!("sequential append failed: {}", code_of(&status)));
            }
        }
    }
    rtts.sort_unstable();
    let seq = format!(
        "sequential flush RTT us: p50={} p90={} p99={}",
        rtts[100], rtts[180], rtts[198]
    );

    // (b) pipelined burst of 16, each flushed: when does each ack arrive?
    let burst = 16usize;
    let begun = std::time::Instant::now();
    for index in 0..burst {
        let message = BidiWriteObjectRequest {
            first_message: None,
            write_offset: tail + (index as i64) * payload.len() as i64,
            data: Some(Data::ChecksummedData(ChecksummedData {
                content: Bytes::copy_from_slice(&payload),
                crc32c: Some(crc32c::crc32c(&payload)),
            })),
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        if held.tx.send(message).await.is_err() {
            cleanup(ctx, &object, generation).await;
            return bad("burst send failed".to_string());
        }
    }
    let burst_end = tail + (burst as i64) * payload.len() as i64;
    let mut first_ack_us = None;
    let last_ack_us = loop {
        match read_persisted(&mut held.resp, tail + payload.len() as i64).await {
            Ok(size) => {
                let at = begun.elapsed().as_micros() as u64;
                if first_ack_us.is_none() {
                    first_ack_us = Some(at);
                }
                if size >= burst_end {
                    break at;
                }
            }
            Err(status) => {
                cleanup(ctx, &object, generation).await;
                return bad(format!("burst ack failed: {}", code_of(&status)));
            }
        }
    };
    tail = burst_end;
    let pipelined = format!(
        "pipelined burst of {burst} (flush each): first ack {}us, last ack {}us",
        first_ack_us.unwrap_or(0),
        last_ack_us
    );

    // (c) the same burst with ONE flush on the final message
    let begun = std::time::Instant::now();
    for index in 0..burst {
        let last = index + 1 == burst;
        let message = BidiWriteObjectRequest {
            first_message: None,
            write_offset: tail + (index as i64) * payload.len() as i64,
            data: Some(Data::ChecksummedData(ChecksummedData {
                content: Bytes::copy_from_slice(&payload),
                crc32c: Some(crc32c::crc32c(&payload)),
            })),
            flush: last,
            state_lookup: last,
            ..Default::default()
        };
        if held.tx.send(message).await.is_err() {
            cleanup(ctx, &object, generation).await;
            return bad("group burst send failed".to_string());
        }
    }
    let group_end = tail + (burst as i64) * payload.len() as i64;
    let group_us = match read_persisted(&mut held.resp, group_end).await {
        Ok(_) => begun.elapsed().as_micros() as u64,
        Err(status) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!("group-flush ack failed: {}", code_of(&status)));
        }
    };
    let group = format!("same burst, ONE flush on last: all durable in {group_us}us");

    drop(held);
    cleanup(ctx, &object, generation).await;
    ok(format!("{seq}; {pipelined}; {group}"))
}

async fn probe_t8(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t8");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let payload: &[u8] = b"flush-probe-data";
    let started = std::time::Instant::now();
    let (mut held, first) = open_held(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        payload,
    )
    .await?;
    let mut tail = match first {
        Ok(size) => size,
        Err(status) => {
            cleanup(ctx, &object, generation).await;
            return bad(format!("first flushed append failed: {}", code_of(&status)));
        }
    };
    let mut completed = 1usize;
    let mut failure = None;
    for _ in 1..100 {
        match send_continuation(&mut held, tail, payload).await {
            Ok(size) => {
                tail = size;
                completed += 1;
            }
            Err(status) => {
                failure = Some(status);
                break;
            }
        }
    }
    let elapsed = started.elapsed();
    drop(held);
    cleanup(ctx, &object, generation).await;
    let detail = match failure {
        None => format!(
            "100/100 per-record flushed appends on one session in {elapsed:.2?} \
             ({:.1}/s) -> flushes are NOT the rate-limited mutation",
            100.0 / elapsed.as_secs_f64()
        ),
        Some(status) => format!(
            "throttled after {completed}/100 flushed appends in {elapsed:.2?}: {} {} \
             -> per-record flushes ARE rate limited",
            code_of(&status),
            status.message()
        ),
    };
    ok(detail)
}

async fn probe_t7(ctx: &Ctx, run_id: u128) -> Result<ProbeResult> {
    let object = format!("{run_id}-t7");
    create_appendable(ctx, &object).await?;
    let (generation, metageneration, size) = stat(ctx, &object).await?;
    let payload: &[u8] = b"0123456789abcdef";
    let tail = append_once(
        ctx,
        &object,
        generation,
        Some(metageneration),
        size,
        payload,
    )
    .await
    .map_err(|status| anyhow::anyhow!("append: {}", code_of(&status)))?;
    let open_read = read_all(ctx, &object, generation).await;
    let (_, _, open_stat_size) = stat(ctx, &object).await?;
    finalize_once(ctx, &object, generation, metageneration, tail)
        .await
        .map_err(|status| anyhow::anyhow!("finalize: {}", code_of(&status)))?;
    let final_read = read_all(ctx, &object, generation).await;
    cleanup(ctx, &object, generation).await;
    let open_desc = match &open_read {
        Ok(bytes) => format!("{bytes} bytes"),
        Err(status) => format!("error {}", code_of(status)),
    };
    let final_desc = match &final_read {
        Ok(bytes) => format!("{bytes} bytes"),
        Err(status) => format!("error {}", code_of(status)),
    };
    let detail = format!(
        "persisted {tail}B flushed; OPEN object: ReadObject -> {open_desc}, GetObject.size -> {open_stat_size}; FINALIZED: ReadObject -> {final_desc}"
    );
    match final_read {
        Ok(bytes) if bytes as i64 == tail => ok(detail),
        _ => bad(detail),
    }
}

async fn read_all(ctx: &Ctx, object: &str, generation: i64) -> Result<usize, Status> {
    let request = ReadObjectRequest {
        bucket: ctx.bucket.clone(),
        object: object.to_string(),
        generation,
        ..Default::default()
    };
    let mut stream = ctx
        .client
        .clone()
        .read_object(ctx.request(request))
        .await?
        .into_inner();
    let mut total = 0usize;
    while let Some(response) = stream.message().await? {
        if let Some(data) = response.checksummed_data {
            total += data.content.len();
        }
    }
    Ok(total)
}

async fn stat(ctx: &Ctx, object: &str) -> Result<(i64, i64, i64)> {
    let object = get_object(ctx, object).await?;
    Ok((object.generation, object.metageneration, object.size))
}

async fn get_object(ctx: &Ctx, object: &str) -> Result<Object> {
    let request = GetObjectRequest {
        bucket: ctx.bucket.clone(),
        object: object.to_string(),
        ..Default::default()
    };
    Ok(ctx
        .client
        .clone()
        .get_object(ctx.request(request))
        .await
        .context("GetObject")?
        .into_inner())
}

async fn bump_metageneration(
    ctx: &Ctx,
    object: &str,
    generation: i64,
    metageneration: i64,
) -> Result<()> {
    let mut metadata = HashMap::new();
    metadata.insert("chorus.probe".to_string(), format!("bump-{metageneration}"));
    let request = UpdateObjectRequest {
        object: Some(Object {
            bucket: ctx.bucket.clone(),
            name: object.to_string(),
            generation,
            metadata,
            ..Default::default()
        }),
        if_generation_match: Some(generation),
        if_metageneration_match: Some(metageneration),
        update_mask: Some(prost_types::FieldMask {
            paths: vec!["metadata".into()],
        }),
        ..Default::default()
    };
    ctx.client
        .clone()
        .update_object(ctx.request(request))
        .await
        .context("UpdateObject (metageneration bump)")?;
    Ok(())
}

/// One-shot append over a freshly opened stream. Returns the persisted size or
/// the gRPC status the open/append produced.
async fn append_once(
    ctx: &Ctx,
    object: &str,
    generation: i64,
    if_metageneration_match: Option<i64>,
    offset: i64,
    data: &[u8],
) -> Result<i64, Status> {
    let message = BidiWriteObjectRequest {
        first_message: Some(FirstMessage::AppendObjectSpec(AppendObjectSpec {
            bucket: ctx.bucket.clone(),
            object: object.to_string(),
            generation,
            if_metageneration_match,
            write_handle: None,
            ..Default::default()
        })),
        write_offset: offset,
        data: Some(Data::ChecksummedData(ChecksummedData {
            content: Bytes::copy_from_slice(data),
            crc32c: Some(crc32c::crc32c(data)),
        })),
        flush: true,
        state_lookup: true,
        ..Default::default()
    };
    let expected = offset + data.len() as i64;
    let mut stream = ctx
        .client
        .clone()
        .bidi_write_object(ctx.request(tokio_stream::iter([message])))
        .await?
        .into_inner();
    read_persisted(&mut stream, expected).await
}

async fn finalize_once(
    ctx: &Ctx,
    object: &str,
    generation: i64,
    if_metageneration_match: i64,
    offset: i64,
) -> Result<(), Status> {
    let message = BidiWriteObjectRequest {
        first_message: Some(FirstMessage::AppendObjectSpec(AppendObjectSpec {
            bucket: ctx.bucket.clone(),
            object: object.to_string(),
            generation,
            if_metageneration_match: Some(if_metageneration_match),
            write_handle: None,
            ..Default::default()
        })),
        write_offset: offset,
        finish_write: true,
        ..Default::default()
    };
    let mut stream = ctx
        .client
        .clone()
        .bidi_write_object(ctx.request(tokio_stream::iter([message])))
        .await?
        .into_inner();
    loop {
        match stream.message().await? {
            Some(response) if matches!(response.write_status, Some(WriteStatus::Resource(_))) => {
                return Ok(())
            }
            Some(_) => {}
            None => return Err(Status::internal("finalize returned no resource")),
        }
    }
}

struct HeldStream {
    tx: mpsc::Sender<BidiWriteObjectRequest>,
    resp: Streaming<BidiWriteObjectResponse>,
}

/// Open a long-lived append stream, send the first append, and return the
/// persisted size (or status) of that first append. The stream stays open.
async fn open_held(
    ctx: &Ctx,
    object: &str,
    generation: i64,
    if_metageneration_match: Option<i64>,
    offset: i64,
    data: &[u8],
) -> Result<(HeldStream, Result<i64, Status>)> {
    let (tx, rx) = mpsc::channel::<BidiWriteObjectRequest>(8);
    let first = BidiWriteObjectRequest {
        first_message: Some(FirstMessage::AppendObjectSpec(AppendObjectSpec {
            bucket: ctx.bucket.clone(),
            object: object.to_string(),
            generation,
            if_metageneration_match,
            write_handle: None,
            ..Default::default()
        })),
        write_offset: offset,
        data: Some(Data::ChecksummedData(ChecksummedData {
            content: Bytes::copy_from_slice(data),
            crc32c: Some(crc32c::crc32c(data)),
        })),
        flush: true,
        state_lookup: true,
        ..Default::default()
    };
    // Buffer the first append BEFORE opening: GCS withholds response headers
    // until it receives a request, so awaiting the open on an empty request
    // stream would deadlock (server waits for a write, client waits for headers).
    if tx.send(first).await.is_err() {
        anyhow::bail!("held stream receiver dropped before first append");
    }
    let open = tokio::time::timeout(
        std::time::Duration::from_secs(READ_TIMEOUT_SECS),
        ctx.client
            .clone()
            .bidi_write_object(ctx.request(ReceiverStream::new(rx))),
    )
    .await;
    let mut resp = match open {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(status)) => return Err(status).context("open held BidiWriteObject stream"),
        Err(_) => {
            anyhow::bail!("open held BidiWriteObject stream timed out after {READ_TIMEOUT_SECS}s")
        }
    };
    let outcome = read_persisted(&mut resp, offset + data.len() as i64).await;
    Ok((HeldStream { tx, resp }, outcome))
}

/// Send a continuation append (no first_message) on an already-open stream.
async fn send_continuation(held: &mut HeldStream, offset: i64, data: &[u8]) -> Result<i64, Status> {
    let message = BidiWriteObjectRequest {
        first_message: None,
        write_offset: offset,
        data: Some(Data::ChecksummedData(ChecksummedData {
            content: Bytes::copy_from_slice(data),
            crc32c: Some(crc32c::crc32c(data)),
        })),
        flush: true,
        state_lookup: true,
        ..Default::default()
    };
    let expected = offset + data.len() as i64;
    if held.tx.send(message).await.is_err() {
        // Server already dropped the request half -> read the terminal status.
        return read_persisted(&mut held.resp, expected).await;
    }
    read_persisted(&mut held.resp, expected).await
}

/// Read responses until a persisted-size of at least `expected` arrives (the
/// post-write ack), bounded so a held-open stream that is neither acked nor
/// aborted cannot hang the probe. GetObject.size does NOT reflect flushed but
/// unfinalized appends, so the persisted-size in these responses is the only
/// authoritative tail offset. A pre-write `state_lookup` value below `expected`
/// is skipped rather than mistaken for the ack.
async fn read_persisted(
    resp: &mut Streaming<BidiWriteObjectResponse>,
    expected: i64,
) -> Result<i64, Status> {
    let read = async {
        loop {
            match resp.message().await {
                Ok(Some(message)) => {
                    if let Some(size) = persisted(&message) {
                        if size >= expected {
                            return Ok(size);
                        }
                    }
                }
                Ok(None) => {
                    return Err(Status::aborted(
                        "stream closed before reaching the expected offset",
                    ))
                }
                Err(status) => return Err(status),
            }
        }
    };
    match tokio::time::timeout(std::time::Duration::from_secs(READ_TIMEOUT_SECS), read).await {
        Ok(result) => result,
        Err(_) => Err(Status::deadline_exceeded(format!(
            "no persisted-size >= {expected} within {READ_TIMEOUT_SECS}s (stream neither acked the write nor aborted)"
        ))),
    }
}

async fn cleanup(ctx: &Ctx, object: &str, generation: i64) {
    if ctx.keep {
        return;
    }
    let request = DeleteObjectRequest {
        bucket: ctx.bucket.clone(),
        object: object.to_string(),
        if_generation_match: Some(generation),
        ..Default::default()
    };
    let _ = ctx.client.clone().delete_object(ctx.request(request)).await;
}
