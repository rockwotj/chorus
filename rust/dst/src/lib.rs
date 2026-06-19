use std::collections::HashSet;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub mod production;
pub mod sim_transport;

pub const TRACE_EVENTS: &[&str] = &[
    "SegmentCreateAttempt",
    "SegmentCreated",
    "SegmentCreateRejected",
    "ProducerSpike",
    "RecordFormed",
    "RecordPersisted",
    "CanonicalPersisted",
    "RecordCommitted",
    "ProducerAcknowledged",
    "RecoveryStarted",
    "RecoverySelected",
    "RecoveryCompleted",
    "DirectoryAdopted",
    "SealedCopyRepaired",
    "SegmentFinalized",
    "SealQuorumEnforced",
    "SegmentSealed",
    "RotationGateReleased",
    "SegmentOpened",
    "TruncationProposed",
    "SegmentDeleted",
    "ReplayOpened",
    "ReplayRecord",
    "ReplayClosed",
    "ZoneCrash",
    "ZoneRestart",
    "DiskCorrupted",
    "WriterCrash",
    "WriterRestart",
    "RpcDropped",
    "RpcUnavailable",
    "RpcDeadlineExceeded",
    "GetSizeObserved",
    "EpochClaimed",
    "ViewCommitted",
    "FloorCommitted",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceEvent {
    pub seq: u64,
    pub time_ms: u64,
    pub event: String,
    pub writer: u64,
    pub epoch: u64,
    pub zone: Option<usize>,
    pub logical_offset: Option<u64>,
    pub value: Option<u64>,
    pub segment: Option<u64>,
    /// Trace-local stable identity for an opaque segment object id.
    pub segment_id: Option<u64>,
    /// Trace-local stable identity of the manifest's current seal object.
    pub current_seal_id: Option<u64>,
    pub tail_base: Option<u64>,
    pub seal_base: Option<u64>,
    pub directory_index: Option<u64>,
    pub directory_len: Option<u64>,
    // Object generation at a segment base: 0 until recovery retires a name
    // and commits a fresh one at the same base.
    pub gen: Option<u64>,
    pub record_end: Option<u64>,
    pub truncation_floor: Option<u64>,
    pub reader: Option<u64>,
    pub reported_size: Option<i64>,
    pub finalized: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SimulationReport {
    pub seed: u64,
    pub steps: u64,
    pub virtual_time_ms: u64,
    pub events: Vec<TraceEvent>,
    pub digest: String,
    pub committed_records: u64,
    pub truncation_floor: u64,
}

/// Validate the provider-neutral JSONL envelope before semantic conformance.
///
/// Protocol invariants intentionally live in the generated PObserve monitors,
/// so this function checks only that Rust emitted declared events in one
/// contiguous sequence. Keeping semantic checks out of Rust prevents the test
/// oracle from silently drifting away from the P specification.
pub fn validate_trace_structure(events: &[TraceEvent]) -> Result<()> {
    let known: HashSet<_> = TRACE_EVENTS.iter().copied().collect();
    let mut last_seq = None;

    for event in events {
        if !known.contains(event.event.as_str()) {
            bail!("unknown transition event {}", event.event);
        }
        if let Some(last) = last_seq {
            if event.seq != last + 1 {
                bail!("non-contiguous trace sequence at {}", event.seq);
            }
        }
        last_seq = Some(event.seq);
    }
    Ok(())
}

pub fn trace_digest(events: &[TraceEvent]) -> Result<String> {
    let mut hasher = Sha256::new();
    for event in events {
        serde_json::to_writer(&mut hasher, event)?;
        hasher.update(b"\n");
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conformance_fixture() -> Vec<TraceEvent> {
        include_str!("../tests/fixtures/trace-events.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn conformance_fixture_covers_the_event_manifest() {
        let events = conformance_fixture();
        validate_trace_structure(&events).unwrap();

        let actual: HashSet<_> = events.iter().map(|event| event.event.as_str()).collect();
        let manifest: Vec<_> = include_str!("../../../p/TRACE_EVENTS.txt")
            .lines()
            .filter(|line| !line.is_empty())
            .collect();
        let expected: HashSet<_> = manifest.iter().copied().collect();
        assert_eq!(actual, expected);
        assert_eq!(events.len(), manifest.len());
        assert_eq!(manifest, TRACE_EVENTS);
    }

    #[test]
    fn structural_validation_rejects_sequence_gaps() {
        let mut events = conformance_fixture();
        events[1].seq += 1;
        assert!(validate_trace_structure(&events).is_err());
    }
}
