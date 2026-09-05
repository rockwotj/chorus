// Inductive transfer proof over unbounded integer prefix boundaries. Quorum
// finalization and immutable storage durability/integrity are premises. This
// proves the publication/eviction protocol, not the Rust implementation or the
// provider. The explicit ArchiveLifecycle model additionally exercises two
// cached proposals, per-zone cleanup, and the shared manifest implementation.
event eArchiveProofStep;
machine ArchiveProofModel {
    var sealedEnd: int;
    var finalizedEnd: int;
    var uploadedEnd: int;
    var indexedEnd: int;
    var archiveEnd: int;
    var hotStart: int;
    var deletedEnd: int;
    var checkpoint: int;
    var version: int;
    var proposalValid: bool;
    var proposalParent: int;
    var proposalEnd: int;
    var proposalVersion: int;
    var readerNext: int;
    var readerDropped: bool;
    var droppedAt: int;
    // Recovery scans an adopted archive prefix followed by its hot suffix.
    // Phase 1 allows publication/CAS refreshes after adoption, before replay.
    var recoveryPhase: int;
    var recoveryArchiveEnd: int;
    var recoveryHotStart: int;
    var recoveryFrom: int;
    var recoveryEnd: int;
    var recoveryNext: int;
    var recoveryDelivered: int;

    start state Running {
        entry { send this, eArchiveProofStep; }
        on eArchiveProofStep do Step;
    }

    fun Step() {
        if ($) {
            // The next fold follows enforcement of the preceding seal.
            finalizedEnd = sealedEnd;
            sealedEnd = sealedEnd + 1;
            version = version + 1;
        } else if ($) {
            if (archiveEnd < finalizedEnd) {
                proposalValid = true;
                proposalParent = archiveEnd;
                proposalEnd = archiveEnd + 1;
                proposalVersion = version;
            }
        } else if ($) {
            // Failures can leave durable bytes but never advance publication.
            if (proposalValid && proposalEnd == uploadedEnd + 1) {
                uploadedEnd = proposalEnd;
            }
        } else if ($) {
            if (proposalValid && proposalEnd <= uploadedEnd &&
                proposalEnd > indexedEnd) { indexedEnd = proposalEnd; }
        } else if ($) {
            // Exact parent/version CAS. A response loss is equivalent to this
            // transition followed by a retry or crash of the worker.
            if (proposalValid && proposalVersion == version &&
                proposalParent == archiveEnd && proposalEnd <= indexedEnd) {
                archiveEnd = proposalEnd;
                hotStart = archiveEnd;
                version = version + 1;
            }
        } else if ($) {
            if (deletedEnd < archiveEnd) { deletedEnd = deletedEnd + 1; }
        } else if ($) {
            checkpoint = sealedEnd;
            version = version + 1;
        } else if ($) {
            // Worker crash discards only volatile proposal state.
            proposalValid = false;
            version = version + 1;
        } else if ($) {
            // Stale hot reads may require refresh and archive fallback. The
            // disjunction is exactly the two durable sources of sealed data.
            if (!readerDropped && readerNext < sealedEnd &&
                (readerNext >= deletedEnd || readerNext < archiveEnd)) {
                readerNext = readerNext + 1;
            }
        } else if ($) {
            readerDropped = true;
            droppedAt = readerNext;
        } else if ($) {
            recoveryPhase = 1;
            recoveryArchiveEnd = archiveEnd;
            recoveryHotStart = hotStart;
            recoveryFrom = checkpoint;
            recoveryEnd = sealedEnd;
            recoveryNext = checkpoint;
            recoveryDelivered = checkpoint;
        } else if (recoveryPhase == 1) {
            // Live manifest refreshes do not change the adopted replay root.
            recoveryPhase = 2;
        } else if (recoveryPhase == 2) {
            if (recoveryNext < recoveryArchiveEnd && recoveryNext < recoveryEnd) {
                assert recoveryNext == recoveryDelivered, "archive replay is not ordered";
                recoveryNext = recoveryNext + 1;
                recoveryDelivered = recoveryDelivered + 1;
            } else {
                recoveryPhase = 3;
                recoveryNext = recoveryHotStart;
                if (recoveryNext < recoveryFrom) { recoveryNext = recoveryFrom; }
            }
        } else if (recoveryPhase == 3) {
            if (recoveryNext < recoveryEnd) {
                assert recoveryNext == recoveryDelivered, "hot replay skipped or duplicated a record";
                recoveryNext = recoveryNext + 1;
                recoveryDelivered = recoveryDelivered + 1;
            } else { recoveryPhase = 4; }
        }
        send this, eArchiveProofStep;
    }
}

init-condition forall (a: ArchiveProofModel) ::
    a.sealedEnd == 1 && a.finalizedEnd == 0 && a.uploadedEnd == 0 &&
    a.indexedEnd == 0 && a.archiveEnd == 0 && a.hotStart == 0 &&
    a.deletedEnd == 0 && a.checkpoint == 0 && a.version == 0 &&
    !a.proposalValid && a.proposalParent == 0 && a.proposalEnd == 0 &&
    a.proposalVersion == 0 && a.readerNext == 0 &&
    !a.readerDropped && a.droppedAt == 0 &&
    a.recoveryPhase == 0 && a.recoveryArchiveEnd == 0 && a.recoveryHotStart == 0 &&
    a.recoveryFrom == 0 && a.recoveryEnd == 0 && a.recoveryNext == 0 &&
    a.recoveryDelivered == 0;

Lemma archive_transfer_inductive {
    invariant archive_prefix_order:
        forall (a: ArchiveProofModel) ::
            0 <= a.deletedEnd && a.deletedEnd <= a.archiveEnd &&
            a.archiveEnd <= a.indexedEnd && a.indexedEnd <= a.uploadedEnd &&
            a.uploadedEnd <= a.finalizedEnd && a.finalizedEnd < a.sealedEnd;
    invariant archive_publication_and_hot_removal_are_atomic:
        forall (a: ArchiveProofModel) :: a.hotStart == a.archiveEnd;
    invariant archive_current_seal_remains_hot:
        forall (a: ArchiveProofModel) :: a.hotStart < a.sealedEnd;
    invariant archive_checkpoint_is_not_eviction_authority:
        forall (a: ArchiveProofModel) ::
            0 <= a.checkpoint && a.checkpoint <= a.sealedEnd &&
            a.deletedEnd <= a.archiveEnd;
    invariant archive_proposal_is_a_finalized_successor:
        forall (a: ArchiveProofModel) :: a.proposalValid ==>
            (0 <= a.proposalParent && a.proposalParent <= a.archiveEnd &&
             a.proposalEnd == a.proposalParent + 1 &&
             a.proposalEnd <= a.finalizedEnd &&
             0 <= a.proposalVersion && a.proposalVersion <= a.version);
    invariant archive_version_is_nonnegative:
        forall (a: ArchiveProofModel) :: a.version >= 0;
    invariant archive_reader_is_bounded:
        forall (a: ArchiveProofModel) ::
            0 <= a.readerNext && a.readerNext <= a.sealedEnd;
    invariant archive_drop_stops_delivery:
        forall (a: ArchiveProofModel) :: a.readerDropped ==>
            a.readerNext == a.droppedAt;
    invariant archive_recovery_has_a_durable_source:
        forall (a: ArchiveProofModel) ::
            a.deletedEnd <= a.archiveEnd && a.archiveEnd <= a.uploadedEnd;
    invariant archive_recovery_snapshot_bounds:
        forall (a: ArchiveProofModel) ::
            0 <= a.recoveryPhase && a.recoveryPhase <= 4 &&
            (a.recoveryPhase != 0 ==>
                (0 <= a.recoveryFrom && a.recoveryFrom <= a.recoveryEnd &&
                 a.recoveryEnd <= a.sealedEnd &&
                 a.recoveryFrom <= a.recoveryDelivered && a.recoveryDelivered <= a.recoveryEnd));
    invariant archive_recovery_snapshot_join:
        forall (a: ArchiveProofModel) :: a.recoveryPhase != 0 ==>
            (0 <= a.recoveryArchiveEnd && a.recoveryArchiveEnd == a.recoveryHotStart &&
             a.recoveryHotStart < a.recoveryEnd);
    invariant archive_recovery_replay_cursor:
        forall (a: ArchiveProofModel) ::
            (a.recoveryPhase == 1 ==>
                (a.recoveryNext == a.recoveryFrom && a.recoveryDelivered == a.recoveryFrom)) &&
            (a.recoveryPhase == 2 ==>
                (a.recoveryNext == a.recoveryDelivered &&
                 (a.recoveryNext <= a.recoveryArchiveEnd || a.recoveryNext == a.recoveryFrom))) &&
            (a.recoveryPhase >= 3 ==>
                (a.recoveryNext == a.recoveryDelivered && a.recoveryNext >= a.recoveryHotStart));
    invariant archive_recovery_completed_range:
        forall (a: ArchiveProofModel) :: a.recoveryPhase == 4 ==>
            a.recoveryDelivered == a.recoveryEnd;
}

Proof {
    prove archive_transfer_inductive;
    prove default using archive_transfer_inductive;
}
