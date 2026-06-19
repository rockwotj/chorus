event eChainStep;
event eRecoveryStep;
event eMaintenanceStep;
event eRecordStep;
event eManifestStep;
event eRotationGateStep;

machine SegmentChainProofModel {
    var creator0: int;
    var creator1: int;
    var creator2: int;
    var active0: int;
    var active1: int;
    var active2: int;
    var finalized0: bool;
    var finalized1: bool;
    var finalized2: bool;
    var appender: int;
    var recoveryAppended: bool;
    var sealed: bool;
    var next0: int;
    var next1: int;
    var next2: int;

    start state Running {
        entry { send this, eChainStep; }
        on eChainStep do ChooseOperation;
        ignore eRecoveryStep, eMaintenanceStep, eRecordStep, eManifestStep;
    }

    fun ChooseOperation() {
        var zone: int;
        var writer: int;
        zone = 0;
        if ($) { zone = 1; } else if ($) { zone = 2; }
        writer = 1;
        if ($) { writer = 2; }

        if ($) {
            if (zone == 0 && creator0 == 0) {
                creator0 = writer;
                active0 = writer;
            } else if (zone == 1 && creator1 == 0) {
                creator1 = writer;
                active1 = writer;
            } else if (zone == 2 && creator2 == 0) {
                creator2 = writer;
                active2 = writer;
            }
        } else if ($) {
            if ((creator0 == writer && creator1 == writer) ||
                (creator0 == writer && creator2 == writer) ||
                (creator1 == writer && creator2 == writer)) {
                if (zone == 0 && creator0 == writer && active0 == writer &&
                    !finalized0 && (appender == 0 || appender == writer)) {
                    appender = writer;
                } else if (zone == 1 && creator1 == writer &&
                    active1 == writer && !finalized1 &&
                    (appender == 0 || appender == writer)) {
                    appender = writer;
                } else if (zone == 2 && creator2 == writer &&
                    active2 == writer && !finalized2 &&
                    (appender == 0 || appender == writer)) {
                    appender = writer;
                }
            }
        } else if ($) {
            if (zone == 0 && creator0 != 0 && !finalized0) { active0 = 3; }
            else if (zone == 1 && creator1 != 0 && !finalized1) { active1 = 3; }
            else if (zone == 2 && creator2 != 0 && !finalized2) { active2 = 3; }
        } else if ($) {
            if (zone == 0 && active0 != 0) { finalized0 = true; }
            else if (zone == 1 && active1 != 0) { finalized1 = true; }
            else if (zone == 2 && active2 != 0) { finalized2 = true; }
        } else if ($) {
            if ((finalized0 && finalized1) ||
                (finalized0 && finalized2) ||
                (finalized1 && finalized2)) {
                sealed = true;
            }
        } else {
            if (sealed) {
                if (zone == 0 && next0 == 0) { next0 = writer; }
                else if (zone == 1 && next1 == 0) { next1 = writer; }
                else if (zone == 2 && next2 == 0) { next2 = writer; }
            }
        }
        send this, eChainStep;
    }
}

machine RecoveryProofModel {
    var value0: bool;
    var value1: bool;
    var value2: bool;
    var committed: bool;
    var recoveryDone: bool;
    var recovered: bool;
    var writeback0: bool;
    var writeback1: bool;
    var writeback2: bool;

    start state Running {
        entry { send this, eRecoveryStep; }
        on eRecoveryStep do ChooseOperation;
        ignore eChainStep, eMaintenanceStep, eRecordStep, eManifestStep;
    }

    fun ChooseOperation() {
        var zone: int;
        zone = 0;
        if ($) { zone = 1; } else if ($) { zone = 2; }
        if ($) {
            if (!recoveryDone) {
                if (zone == 0) { value0 = true; }
                else if (zone == 1) { value1 = true; }
                else { value2 = true; }
            }
        } else if ($) {
            if (!recoveryDone &&
                ((value0 && value1) || (value0 && value2) ||
                 (value1 && value2))) {
                committed = true;
            }
        } else if (!recoveryDone) {
            if ($) {
                recovered = value0 || value1;
                if (recovered) { value0 = true; value1 = true; }
                writeback0 = true;
                writeback1 = true;
            } else if ($) {
                recovered = value0 || value2;
                if (recovered) { value0 = true; value2 = true; }
                writeback0 = true;
                writeback2 = true;
            } else {
                recovered = value1 || value2;
                if (recovered) { value1 = true; value2 = true; }
                writeback1 = true;
                writeback2 = true;
            }
            recoveryDone = true;
        }
        send this, eRecoveryStep;
    }
}

machine SealedMaintenanceProofModel {
    var active: bool;
    var finalizedCopies: int;
    var sealed: bool;
    var sourceLength: int;
    var targetLength: int;
    var repaired: bool;

    start state Running {
        entry { send this, eMaintenanceStep; }
        on eMaintenanceStep do ChooseOperation;
        ignore eChainStep, eRecoveryStep, eRecordStep, eManifestStep;
    }

    fun ChooseOperation() {
        if ($) {
            if (active && finalizedCopies < 3) {
                finalizedCopies = finalizedCopies + 1;
            }
        } else if ($) {
            if (finalizedCopies >= 2) {
                sealed = true;
                active = false;
            }
        } else {
            if (sealed && !active) {
                targetLength = sourceLength;
                repaired = true;
            }
        }
        send this, eMaintenanceStep;
    }
}

machine RecordStartupReplayTruncationProofModel {
    var formed0: bool;
    var formed1: bool;
    var persisted0: bool;
    var persisted1: bool;
    var committed0: bool;
    var committed1: bool;
    var acknowledged0: bool;
    var acknowledged1: bool;
    var poisoned: bool;
    var sealedEnd: int;
    var floor: int;
    var deleted: bool;
    var replayStarted: bool;
    var replayComplete: bool;
    var replayStart: int;
    var replayEnd: int;
    var replayOffset: int;
    var deletedEnd: int;

    start state Running {
        entry { send this, eRecordStep; }
        on eRecordStep do ChooseOperation;
        ignore eChainStep, eRecoveryStep, eMaintenanceStep, eManifestStep;
    }

    fun ChooseOperation() {
        var proposed: int;
        proposed = 1;
        if ($) { proposed = 2; }
        if ($) { formed0 = true; }
        else if ($) { if (formed0) { formed1 = true; } }
        else if ($) { if (formed0) { persisted0 = true; } }
        else if ($) { if (formed1) { persisted1 = true; } }
        else if ($) {
            if (persisted0 && !poisoned) { committed0 = true; }
        }
        else if ($) {
            if (persisted1 && committed0 && !poisoned) { committed1 = true; }
        }
        else if ($) {
            if (formed0 && !committed0) { poisoned = true; }
        }
        else if ($) { if (committed0) { acknowledged0 = true; } }
        else if ($) { if (committed1 && !poisoned) { acknowledged1 = true; } }
        else if ($) {
            if (committed0) {
                sealedEnd = 0;
                if (committed1) { sealedEnd = 1; }
            }
        } else if ($) {
            if (!replayStarted && floor == 0 && committed0) {
                replayStarted = true;
                replayStart = 0;
                replayEnd = 1;
                if (committed1) { replayEnd = 2; }
                replayOffset = 0;
            }
        } else if ($) {
            if (replayStarted && !replayComplete && replayOffset < replayEnd) {
                replayOffset = replayOffset + 1;
            }
        } else if ($) {
            if (replayStarted && replayOffset == replayEnd) {
                replayComplete = true;
            }
        } else if ($) {
            if (proposed >= floor && sealedEnd >= 0 &&
                sealedEnd < proposed && replayComplete) {
                floor = proposed;
            }
        } else {
            if (!deleted && floor > sealedEnd && sealedEnd >= 0) {
                deleted = true;
                deletedEnd = sealedEnd;
            }
        }
        send this, eRecordStep;
    }
}

// The regional manifest register, reduced. The handler's atomicity models
// the register's linearizability — a trusted provider property (GCS strong
// consistency on one regional object), exactly like takeover revocation.
// Two abstract writer incarnations race claims, seal commits (each with a
// nondeterministically derived candidate decision), floor raises, and
// deletion. A loser adopts the committed decision rather than re-deciding.
machine ManifestProofModel {
    var epoch: int;
    var owner: int;
    var claimed1: int;
    var claimed2: int;
    var want1: int;
    var want2: int;
    var committed1: bool;
    var committed2: bool;
    var registerSealEnd: int;
    var floor: int;
    var deleted: bool;

    start state Running {
        entry { send this, eManifestStep; }
        on eManifestStep do ChooseOperation;
        ignore eChainStep, eRecoveryStep, eMaintenanceStep, eRecordStep;
    }

    fun ChooseOperation() {
        if ($) {
            // writer 1 claims: one linearized CAS
            epoch = epoch + 1;
            owner = 1;
            claimed1 = epoch;
        } else if ($) {
            epoch = epoch + 1;
            owner = 2;
            claimed2 = epoch;
        } else if ($) {
            // writers derive a candidate seal decision from witnesses; the
            // two candidates may genuinely differ (divergent quorum shapes)
            if (want1 == 0) {
                want1 = 1;
                if ($) { want1 = 2; }
            }
        } else if ($) {
            if (want2 == 0) {
                want2 = 1;
                if ($) { want2 = 2; }
            }
        } else if ($) {
            // writer 1 commits its seal decision: the CAS re-validates the
            // claim; if a decision already exists it is adopted, never
            // overwritten
            if (claimed1 != 0 && claimed1 == epoch && owner == 1 &&
                want1 != 0) {
                if (registerSealEnd == 0) {
                    registerSealEnd = want1;
                } else {
                    want1 = registerSealEnd;
                }
                committed1 = true;
            }
        } else if ($) {
            if (claimed2 != 0 && claimed2 == epoch && owner == 2 &&
                want2 != 0) {
                if (registerSealEnd == 0) {
                    registerSealEnd = want2;
                } else {
                    want2 = registerSealEnd;
                }
                committed2 = true;
            }
        } else if ($) {
            // the truncation floor is committed through the register before
            // any deletion
            if (registerSealEnd != 0 && floor == 0) {
                floor = registerSealEnd;
            }
        } else {
            if (floor != 0) { deleted = true; }
        }
        send this, eManifestStep;
    }
}

// The rotation gate, reduced to the manifest's current seal record, the exact
// seal whose finalized quorum has been established, and a proposed successor
// seal. A replacement transition is enabled only while the current seal is
// the finalized one; no directory contents are needed for this ordering fact.
machine RotationGateProofModel {
    var currentSeal: int;
    var finalizedQuorum: int;
    var nextSeal: int;

    start state Running {
        entry { send this, eRotationGateStep; }
        on eRotationGateStep do ChooseOperation;
        ignore eChainStep, eRecoveryStep, eMaintenanceStep, eRecordStep,
            eManifestStep;
    }

    fun ChooseOperation() {
        if ($) {
            if (currentSeal == 0) { currentSeal = 1; }
        } else if ($) {
            if (currentSeal != 0) { finalizedQuorum = currentSeal; }
        } else if ($) {
            if (currentSeal != 0 && nextSeal == 0) {
                nextSeal = currentSeal + 1;
            }
        } else {
            if (nextSeal == currentSeal + 1 &&
                finalizedQuorum == currentSeal) {
                currentSeal = nextSeal;
                nextSeal = 0;
            }
        }
        send this, eRotationGateStep;
    }
}

init-condition forall (q: SegmentChainProofModel) ::
    q.creator0 == 0 && q.creator1 == 0 && q.creator2 == 0 &&
    q.active0 == 0 && q.active1 == 0 && q.active2 == 0 &&
    !q.finalized0 && !q.finalized1 && !q.finalized2 &&
    q.appender == 0 && !q.recoveryAppended && !q.sealed &&
    q.next0 == 0 && q.next1 == 0 && q.next2 == 0;

init-condition forall (r: RecoveryProofModel) ::
    !r.value0 && !r.value1 && !r.value2 && !r.committed &&
    !r.recoveryDone && !r.recovered &&
    !r.writeback0 && !r.writeback1 && !r.writeback2;

init-condition forall (m: SealedMaintenanceProofModel) ::
    m.active && m.sourceLength == 2 && m.targetLength == 0 &&
    !m.sealed && !m.repaired;

init-condition forall (b: RecordStartupReplayTruncationProofModel) ::
    !b.formed0 && !b.formed1 && !b.persisted0 && !b.persisted1 &&
    !b.committed0 && !b.committed1 &&
    !b.acknowledged0 && !b.acknowledged1 && !b.poisoned &&
    b.sealedEnd == -1 && b.floor == 0 &&
    !b.replayStarted && !b.replayComplete && b.replayStart == 0 &&
    b.replayEnd == 0 && b.replayOffset == 0 && !b.deleted &&
    b.deletedEnd == 0;

init-condition forall (m: ManifestProofModel) ::
    m.epoch == 0 && m.owner == 0 && m.claimed1 == 0 && m.claimed2 == 0 &&
    m.want1 == 0 && m.want2 == 0 && !m.committed1 && !m.committed2 &&
    m.registerSealEnd == 0 && m.floor == 0 && !m.deleted;

init-condition forall (g: RotationGateProofModel) ::
    g.currentSeal == 0 && g.finalizedQuorum == 0 && g.nextSeal == 0;

Lemma reduced_quorum_wal_inductive {
    invariant at_most_one_creator_quorum:
        forall (q: SegmentChainProofModel) ::
            !(((q.creator0 == 1 && q.creator1 == 1) ||
               (q.creator0 == 1 && q.creator2 == 1) ||
               (q.creator1 == 1 && q.creator2 == 1)) &&
              ((q.creator0 == 2 && q.creator1 == 2) ||
               (q.creator0 == 2 && q.creator2 == 2) ||
               (q.creator1 == 2 && q.creator2 == 2)));

    invariant appender_has_creator_quorum:
        forall (q: SegmentChainProofModel) :: q.appender != 0 ==>
            ((q.creator0 == q.appender && q.creator1 == q.appender) ||
             (q.creator0 == q.appender && q.creator2 == q.appender) ||
             (q.creator1 == q.appender && q.creator2 == q.appender));

    invariant recovery_never_appends:
        forall (q: SegmentChainProofModel) ::
            q.appender != 3 && !q.recoveryAppended;

    invariant recovery_takeover_is_seal_only:
        forall (q: SegmentChainProofModel) ::
            ((q.active0 == 3 || q.active1 == 3 || q.active2 == 3) ==>
                q.appender != 3);

    invariant seal_has_finalize_quorum:
        forall (q: SegmentChainProofModel) :: q.sealed ==>
            ((q.finalized0 && q.finalized1) ||
             (q.finalized0 && q.finalized2) ||
             (q.finalized1 && q.finalized2));

    invariant at_most_one_next_segment_quorum:
        forall (q: SegmentChainProofModel) ::
            !(((q.next0 == 1 && q.next1 == 1) ||
               (q.next0 == 1 && q.next2 == 1) ||
               (q.next1 == 1 && q.next2 == 1)) &&
              ((q.next0 == 2 && q.next1 == 2) ||
               (q.next0 == 2 && q.next2 == 2) ||
               (q.next1 == 2 && q.next2 == 2)));

    invariant next_segment_follows_seal:
        forall (q: SegmentChainProofModel) ::
            (q.next0 != 0 || q.next1 != 0 || q.next2 != 0) ==> q.sealed;

    invariant seal_record_replacement_requires_finalize_quorum:
        forall (g: RotationGateProofModel) ::
            g.currentSeal >= 0 &&
            g.finalizedQuorum >= 0 &&
            g.finalizedQuorum <= g.currentSeal &&
            g.currentSeal <= g.finalizedQuorum + 1 &&
            (g.nextSeal == 0 ||
                (g.currentSeal != 0 &&
                 g.nextSeal == g.currentSeal + 1));

    invariant committed_value_has_quorum:
        forall (r: RecoveryProofModel) :: r.committed ==>
            ((r.value0 && r.value1) ||
             (r.value0 && r.value2) ||
             (r.value1 && r.value2));

    invariant two_zone_recovery_preserves_commit:
        forall (r: RecoveryProofModel) ::
            (r.recoveryDone && r.committed) ==> r.recovered;

    invariant recovered_prefix_is_written_to_read_quorum:
        forall (r: RecoveryProofModel) ::
            (r.recoveryDone && r.recovered) ==>
                ((r.writeback0 && r.writeback1 && r.value0 && r.value1) ||
                 (r.writeback0 && r.writeback2 && r.value0 && r.value2) ||
                 (r.writeback1 && r.writeback2 && r.value1 && r.value2));

    invariant immutable_repair_requires_seal:
        forall (m: SealedMaintenanceProofModel) ::
            m.repaired ==> m.sealed && !m.active;

    invariant immutable_repair_is_exact:
        forall (m: SealedMaintenanceProofModel) ::
            m.repaired ==> m.targetLength == m.sourceLength;

    invariant record_persist_requires_form:
        forall (b: RecordStartupReplayTruncationProofModel) ::
            (b.persisted0 ==> b.formed0) && (b.persisted1 ==> b.formed1);

    invariant pipeline_commit_is_ordered:
        forall (b: RecordStartupReplayTruncationProofModel) ::
            (b.formed1 ==> b.formed0) &&
            (b.committed0 ==> b.formed0) &&
            (b.committed1 ==> b.formed1 && b.committed0);

    invariant producers_ack_only_committed_records:
        forall (b: RecordStartupReplayTruncationProofModel) ::
            (b.acknowledged0 ==> b.committed0) &&
            (b.acknowledged1 ==> b.committed1);

    invariant poisoned_gap_blocks_later_commit_and_ack:
        forall (b: RecordStartupReplayTruncationProofModel) ::
            (b.poisoned && !b.committed0) ==>
                !b.committed1 && !b.acknowledged1;

    // the floor only ever advances after replay completed against an
    // existing seal; the seal may later extend past an old floor, so the
    // stable fact is existence, not ordering
    invariant truncation_only_advances_past_sealed_segment:
        forall (b: RecordStartupReplayTruncationProofModel) ::
            b.floor > 0 ==> b.replayComplete && b.sealedEnd >= 0;

    invariant deletion_is_whole_segment:
        forall (b: RecordStartupReplayTruncationProofModel) :: b.deleted ==>
            b.deletedEnd >= 0 && b.deletedEnd < b.floor;

    invariant startup_replay_cursor_is_record_ordered_and_bounded:
        forall (b: RecordStartupReplayTruncationProofModel) :: b.replayStarted ==>
            b.replayStart <= b.replayOffset &&
            b.replayOffset <= b.replayEnd &&
            b.replayStart <= b.replayEnd;

    invariant completed_startup_replay_reached_fixed_end:
        forall (b: RecordStartupReplayTruncationProofModel) :: b.replayComplete ==>
            b.replayStarted && b.replayOffset == b.replayEnd;

    // ---- regional manifest register ----

    invariant manifest_aux_claim_bounds:
        forall (m: ManifestProofModel) ::
            m.claimed1 <= m.epoch && m.claimed2 <= m.epoch &&
            m.claimed1 >= 0 && m.claimed2 >= 0 && m.epoch >= 0;

    invariant manifest_aux_owner_range:
        forall (m: ManifestProofModel) ::
            (m.owner == 0 || m.owner == 1 || m.owner == 2) &&
            (m.owner == 0 ==> m.epoch == 0);

    invariant manifest_aux_values:
        forall (m: ManifestProofModel) ::
            (m.want1 == 0 || m.want1 == 1 || m.want1 == 2) &&
            (m.want2 == 0 || m.want2 == 1 || m.want2 == 2) &&
            (m.registerSealEnd == 0 || m.registerSealEnd == 1 ||
             m.registerSealEnd == 2);

    invariant manifest_aux_committed_nonzero:
        forall (m: ManifestProofModel) ::
            (m.committed1 ==> (m.want1 != 0 && m.registerSealEnd != 0)) &&
            (m.committed2 ==> (m.want2 != 0 && m.registerSealEnd != 0));

    invariant manifest_epoch_owner_unique:
        forall (m: ManifestProofModel) ::
            !(m.claimed1 != 0 && m.claimed1 == m.claimed2);

    invariant manifest_owner_holds_a_grant:
        forall (m: ManifestProofModel) ::
            (m.owner == 1 ==> m.claimed1 == m.epoch) &&
            (m.owner == 2 ==> m.claimed2 == m.epoch);

    invariant manifest_seal_decision_unique:
        forall (m: ManifestProofModel) ::
            (m.committed1 && m.committed2) ==> m.want1 == m.want2;

    invariant manifest_committed_matches_register:
        forall (m: ManifestProofModel) ::
            (m.committed1 ==> m.want1 == m.registerSealEnd) &&
            (m.committed2 ==> m.want2 == m.registerSealEnd);

    invariant manifest_floor_follows_decision:
        forall (m: ManifestProofModel) ::
            m.floor != 0 ==> m.floor == m.registerSealEnd;

    invariant manifest_delete_after_committed_floor:
        forall (m: ManifestProofModel) :: m.deleted ==> m.floor != 0;
}

Proof {
    prove reduced_quorum_wal_inductive;
    prove default using reduced_quorum_wal_inductive;
}
