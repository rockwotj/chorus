spec QuorumLinearizability observes eRecordPersisted, eCanonicalPersisted,
    eRecordCommitted, eRecoverySelected, eRecoveryCompleted {
    var support: map[(offset: int, value: int, segment: int), set[int]];
    var canonicalSupport: map[(offset: int, value: int, segment: int), set[int]];
    var committed: map[int, tRecord];
    var selected: map[(writerId: int, offset: int), tRecord];

    start state Observing {
        on eRecordPersisted do (payload: (
            zone: int, writerId: int, record: tRecord, gen: int
        )) { AddSupport(payload.zone, payload.record); }

        on eCanonicalPersisted do (payload: (
            zone: int, writerId: int, record: tRecord
        )) {
            var key: (offset: int, value: int, segment: int);
            AddSupport(payload.zone, payload.record);
            key = RecordKey(payload.record);
            if (!(key in canonicalSupport)) {
                canonicalSupport[key] = default(set[int]);
            }
            canonicalSupport[key] += (payload.zone);
        }

        on eRecordCommitted do (payload: (writerId: int, record: tRecord)) {
            var key: (offset: int, value: int, segment: int);
            key = RecordKey(payload.record);
            assert key in support && sizeof(support[key]) >= 2,
                "client acknowledged without two durable replicas";
            if (payload.record.offset in committed) {
                assert committed[payload.record.offset] == payload.record,
                    "two values committed at one logical offset";
            } else {
                assert payload.record.offset == sizeof(committed),
                    "commits must form a contiguous prefix";
                committed[payload.record.offset] = payload.record;
            }
        }

        on eRecoverySelected do (payload: (
            writerId: int, record: tRecord
        )) {
            if (payload.record.offset in committed) {
                assert committed[payload.record.offset] == payload.record,
                    "recovery changed a committed record";
            }
            selected[(writerId=payload.writerId,
                offset=payload.record.offset)] = payload.record;
        }

        on eRecoveryCompleted do (payload: (
            writerId: int,
            segment: int,
            startOffset: int,
            endOffset: int
        )) {
            var offset: int;
            var key: (offset: int, value: int, segment: int);
            foreach (offset in keys(committed)) {
                // no acked-data loss at seal time: every committed record of
                // the recovered segment lies below the recovered end —
                // recovery can promote, never truncate, acknowledged data
                if (committed[offset].segment == payload.segment) {
                    assert offset < payload.endOffset,
                        "recovery sealed below a committed record";
                }
                if (offset >= payload.startOffset && offset < payload.endOffset) {
                    assert (writerId=payload.writerId, offset=offset) in selected &&
                        selected[(writerId=payload.writerId, offset=offset)] == committed[offset],
                        "startup recovery omitted or changed a committed record";
                    if (committed[offset].segment == payload.segment) {
                        key = RecordKey(committed[offset]);
                        assert key in canonicalSupport &&
                            sizeof(canonicalSupport[key]) >= 2,
                            "recovery did not leave the highest canonical prefix on two witnesses";
                    }
                }
            }
        }
    }

    fun RecordKey(record: tRecord): (offset: int, value: int, segment: int) {
        return (offset=record.offset, value=record.value,
            segment=record.segment);
    }

    fun AddSupport(zone: int, record: tRecord) {
        var key: (offset: int, value: int, segment: int);
        key = RecordKey(record);
        if (!(key in support)) { support[key] = default(set[int]); }
        support[key] += (zone);
    }
}

// Model-only manifest-registration invariant. The acknowledgment event carries
// the opaque object id because tRecord.segment is the logical base, not the
// object identity. Every acknowledgment must observe that id in the most
// recent register-linearized directory, tail, or pending slot.
spec PendingRegistrationSafety observes eManifestCommitted,
    eRecordAcknowledged {
    var durableIds: set[int];
    var haveManifest: bool;

    start state Observing {
        on eManifestCommitted do (record: tManifestRecord) {
            var index: int;
            durableIds = default(set[int]);
            durableIds += (record.tailGen);
            index = 0;
            while (index < sizeof(record.directory)) {
                durableIds += (record.directory[index].id);
                index = index + 1;
            }
            if (record.pending >= 0) {
                durableIds += (record.pending);
            }
            haveManifest = true;
        }

        on eRecordAcknowledged do (payload: (
            writerId: int, record: tRecord, segmentId: int
        )) {
            assert haveManifest && payload.segmentId in durableIds,
                "record acknowledged before its segment id was durable in the manifest";
        }
    }
}

// Model-only whole-chain recovery property for [tail, pending?]. Directory
// records below directoryEnd are authoritative by adoption; every later
// acknowledged record must be selected from a fenced quorum walk. The same
// checks run again for acknowledgments racing after recovery completion.
spec PendingRecoveryCompleteness observes eRecordAcknowledged,
    eRecoverySelected, eDirectoryAdopted, ePendingRecoveryCompleted {
    var acknowledged: map[int, tRecord];
    var selected: map[(writerId: int, offset: int), tRecord];
    var adoptedDirectoryEnd: map[int, int];
    var completed: bool;
    var recoveryWriter: int;
    var directoryEnd: int;
    var recoveredEnd: int;

    start state Observing {
        on eRecordAcknowledged do (payload: (
            writerId: int, record: tRecord, segmentId: int
        )) {
            if (payload.record.offset in acknowledged) {
                assert acknowledged[payload.record.offset] == payload.record,
                    "two acknowledged values occupied one logical offset";
            } else {
                acknowledged[payload.record.offset] = payload.record;
            }
            if (completed) {
                CheckRecovered(payload.record);
            }
        }

        on eRecoverySelected do (payload: (
            writerId: int, record: tRecord
        )) {
            selected[(writerId=payload.writerId,
                offset=payload.record.offset)] = payload.record;
        }

        on eDirectoryAdopted do (payload: (
            writerId: int,
            directoryEntry: tDirectoryEntry,
            entryIndex: int,
            entryCount: int,
            endOffset: int,
            tailBase: int,
            currentSealBase: int,
            currentSealId: int,
            trunc: int
        )) {
            if (!(payload.writerId in adoptedDirectoryEnd) ||
                adoptedDirectoryEnd[payload.writerId] <
                    payload.endOffset + 1) {
                adoptedDirectoryEnd[payload.writerId] =
                    payload.endOffset + 1;
            }
        }

        on ePendingRecoveryCompleted do (payload: (
            writerId: int,
            directoryEnd: int,
            recoveredEnd: int,
            pendingExhausted: bool
        )) {
            var offset: int;
            assert payload.directoryEnd <= payload.recoveredEnd,
                "pending recovery regressed below the adopted directory";
            if (payload.directoryEnd > 0) {
                assert payload.writerId in adoptedDirectoryEnd &&
                    adoptedDirectoryEnd[payload.writerId] >=
                        payload.directoryEnd,
                    "pending recovery counted a directory range it did not adopt";
            }
            completed = true;
            recoveryWriter = payload.writerId;
            directoryEnd = payload.directoryEnd;
            recoveredEnd = payload.recoveredEnd;
            foreach (offset in keys(acknowledged)) {
                CheckRecovered(acknowledged[offset]);
            }
        }
    }

    fun CheckRecovered(record: tRecord) {
        assert record.offset < recoveredEnd,
            "pending recovery lost an acknowledged record";
        if (record.offset >= directoryEnd) {
            assert (writerId=recoveryWriter,
                offset=record.offset) in selected &&
                selected[(writerId=recoveryWriter,
                    offset=record.offset)] == record,
                "pending recovery omitted or changed an acknowledged record";
        }
    }
}

// A takeover-fenced empty frontier may be reused by the recovery writer. The
// safety property is therefore one durable appender per object generation,
// not that a recovery writer can never append to an existing object.
spec SingleWriterPerSegment observes eSegmentCreated, eSegmentOpened,
    eRecordPersisted, eRecoveryStarted {
    // Identities and the appender map are keyed by (segment, gen): two tail
    // generations at one base are two distinct object names, and a fresh
    // generation legitimately gets a fresh creator and a fresh appender
    // after the old name is retired through the register.
    var creationSupport: map[(segment: int, writerId: int, epoch: int, gen: int), set[int]];
    var identities: set[(segment: int, writerId: int, epoch: int, gen: int)];
    var appender: map[(segment: int, gen: int), int];
    start state Observing {
        on eSegmentCreated do (payload: (
            zone: int, writerId: int, epoch: int, segment: int, gen: int
        )) {
            var identity: (segment: int, writerId: int, epoch: int, gen: int);
            var candidate: (segment: int, writerId: int, epoch: int, gen: int);
            var quorumCreators: int;
            identity = (segment=payload.segment,
                writerId=payload.writerId, epoch=payload.epoch,
                gen=payload.gen);
            identities += (identity);
            if (!(identity in creationSupport)) {
                creationSupport[identity] = default(set[int]);
            }
            creationSupport[identity] += (payload.zone);
            quorumCreators = 0;
            foreach (candidate in identities) {
                if (candidate.segment == payload.segment &&
                    candidate.gen == payload.gen &&
                    sizeof(creationSupport[candidate]) >= 2) {
                    quorumCreators = quorumCreators + 1;
                }
            }
            assert quorumCreators <= 1,
                "two writer incarnations conditionally created one segment quorum";
        }

        on eSegmentOpened do (payload: (
            segment: int, writerId: int, epoch: int, gen: int
        )) {
            var identity: (segment: int, writerId: int, epoch: int, gen: int);
            identity = (segment=payload.segment,
                writerId=payload.writerId, epoch=payload.epoch,
                gen=payload.gen);
            assert identity in creationSupport &&
                sizeof(creationSupport[identity]) >= 2,
                "segment opened without a conditional-create quorum";
        }

        on eRecoveryStarted do {}

        on eRecordPersisted do (payload: (
            zone: int, writerId: int, record: tRecord, gen: int
        )) {
            var key: (segment: int, gen: int);
            key = (segment=payload.record.segment, gen=payload.gen);
            if (key in appender) {
                assert appender[key] == payload.writerId,
                    "two writers appended durable data to one segment";
            } else {
                appender[key] = payload.writerId;
            }
        }
    }
}

spec SealAndPrefixSafety observes eSegmentFinalized, eRecordFormed,
    eRecoverySelected, eSegmentSealed, eSealedCopyRepaired {
    var known: set[int];
    var finalized: map[(segment: int, endOffset: int), set[int]];
    var sealed: map[int, int];

    start state Observing {
        on eRecordFormed do (payload: (writerId: int, record: tRecord)) {
            known += (payload.record.offset);
        }

        on eRecoverySelected do (payload: (
            writerId: int, record: tRecord
        )) { known += (payload.record.offset); }

        on eSegmentFinalized do (payload: (
            zone: int, segment: int, validEnd: int
        )) {
            var key: (segment: int, endOffset: int);
            key = (segment=payload.segment, endOffset=payload.validEnd);
            if (!(key in finalized)) {
                finalized[key] = default(set[int]);
            }
            finalized[key] += (payload.zone);
        }

        on eSegmentSealed do (payload: (
            segment: int, endOffset: int
        )) {
            var key: (segment: int, endOffset: int);
            var offset: int;
            key = (segment=payload.segment, endOffset=payload.endOffset);
            assert key in finalized && sizeof(finalized[key]) >= 2,
                "segment seal lacks an exact finalize quorum";
            offset = payload.segment;
            while (offset <= payload.endOffset) {
                assert offset in known,
                    "sealed valid region has an interior gap";
                offset = offset + 1;
            }
            sealed[payload.segment] = payload.endOffset;
        }

        on eSealedCopyRepaired do (payload: (
            zone: int, segment: int, endOffset: int, recordCount: int
        )) {
            assert payload.segment in sealed &&
                sealed[payload.segment] == payload.endOffset &&
                payload.recordCount == payload.endOffset - payload.segment + 1,
                "repair copied an active, partial, or differently sealed object";
        }
    }
}

spec StartupReplayAndTruncation observes eRecordFormed,
    eRecoverySelected, eRecordCommitted, eProducerAck, eSegmentSealed,
    eTruncationProposed, eSegmentDeleted, eReplayOpened, eReplayRecord,
    eReplayClosed {
    var formed: map[int, tRecord];
    var committed: set[int];
    var acknowledgements: set[int];
    var segmentEnds: map[int, int];
    var floor: int;
    var activeReaders: set[int];
    var replayStart: map[int, int];
    var replayEnd: map[int, int];
    var replayNextOffset: map[int, int];

    start state Observing {
        on eRecordFormed do (payload: (writerId: int, record: tRecord)) {
            assert sizeof(activeReaders) == 0,
                "append admission overlapped startup replay";
            assert !(payload.record.offset in formed),
                "duplicate formed record";
            formed[payload.record.offset] = payload.record;
        }

        on eRecoverySelected do (payload: (
            writerId: int, record: tRecord
        )) {
            if (payload.record.offset in formed) {
                assert formed[payload.record.offset] == payload.record,
                    "recovery changed an already formed record";
            } else {
                formed[payload.record.offset] = payload.record;
            }
        }

        on eRecordCommitted do (payload: (writerId: int, record: tRecord)) {
            assert payload.record.offset in formed &&
                formed[payload.record.offset] == payload.record,
                "committed record differs from its formed value";
            assert payload.record.offset == sizeof(committed),
                "pipelined commits became visible out of order";
            committed += (payload.record.offset);
        }

        on eProducerAck do (payload: (writerId: int, offset: int)) {
            assert payload.offset in committed,
                "producer acknowledged an uncommitted record";
            assert !(payload.offset in acknowledgements),
                "producer record acknowledged twice";
            acknowledgements += (payload.offset);
        }

        on eSegmentSealed do (payload: (
            segment: int, endOffset: int
        )) { segmentEnds[payload.segment] = payload.endOffset; }

        on eTruncationProposed do (proposed: int) {
            assert sizeof(activeReaders) == 0,
                "truncation overlapped startup replay";
            assert proposed >= floor, "truncation floor regressed";
        }

        on eSegmentDeleted do (payload: (
            zone: int, segment: int, endOffset: int, floor: int
        )) {
            assert payload.segment in segmentEnds &&
                segmentEnds[payload.segment] == payload.endOffset &&
                payload.endOffset < payload.floor,
                "truncation deleted an unsealed or uncovered segment";
            if (payload.floor > floor) { floor = payload.floor; }
        }

        on eReplayOpened do (payload: (
            reader: int, startOffset: int, endOffset: int
        )) {
            assert !(payload.reader in activeReaders) &&
                payload.startOffset >= floor &&
                payload.startOffset <= payload.endOffset &&
                payload.endOffset <= sizeof(formed),
                "invalid startup replay boundary";
            activeReaders += (payload.reader);
            replayStart[payload.reader] = payload.startOffset;
            replayEnd[payload.reader] = payload.endOffset;
            replayNextOffset[payload.reader] = payload.startOffset;
        }

        on eReplayRecord do (payload: (reader: int, offset: int)) {
            assert payload.reader in activeReaders &&
                payload.offset == replayNextOffset[payload.reader] &&
                payload.offset < replayEnd[payload.reader] &&
                payload.offset in formed,
                "startup replay skipped, duplicated, or crossed its fixed end";
            replayNextOffset[payload.reader] = payload.offset + 1;
        }

        on eReplayClosed do (reader: int) {
            assert reader in activeReaders &&
                replayNextOffset[reader] == replayEnd[reader],
                "startup replay closed before its fixed end";
            activeReaders -= (reader);
        }
    }
}

// A readonly follower is a non-coordinating observer of sealed directory
// entries and the manifest-selected active tail. Sealed records come from the
// published immutable prefix. Active records must be complete, identical
// observations on a strict majority, which makes that prefix recovery-stable
// by quorum intersection even if writer acknowledgment has not yet run.
spec ReadonlyFollowerSafety observes eRecordPersisted, eRecordCommitted,
    eEpochClaimed, eReadonlyOpened, eReadonlySnapshot,
    eReadonlyActiveSnapshot, eReadonlyRecord, eReadonlyLagged {
    var support: map[(offset: int, value: int, segment: int), set[int]];
    var committed: map[int, tRecord];
    var next: map[int, int];
    var readers: set[int];
    var lagged: set[int];
    var snapshotEnd: map[int, int];
    var snapshotTrunc: map[int, int];
    var snapshotSegmentBase: map[int, int];
    var snapshotSegmentId: map[int, int];
    var snapshotSegmentEnd: map[int, int];
    var snapshotActive: map[int, bool];

    start state Observing {
        on eRecordPersisted do (payload: (
            zone: int, writerId: int, record: tRecord, gen: int
        )) {
            var key: (offset: int, value: int, segment: int);
            key = RecordKey(payload.record);
            if (!(key in support)) {
                support[key] = default(set[int]);
            }
            support[key] += (payload.zone);
        }

        on eRecordCommitted do (payload: (
            writerId: int, record: tRecord
        )) {
            committed[payload.record.offset] = payload.record;
        }

        on eEpochClaimed do (payload: (epoch: int, writerId: int)) {
            assert !(payload.writerId in readers),
                "readonly follower claimed a writer epoch";
        }

        on eReadonlyOpened do (payload: (
            reader: int, nextOffset: int
        )) {
            assert !(payload.reader in readers),
                "readonly follower opened twice";
            readers += (payload.reader);
            next[payload.reader] = payload.nextOffset;
        }

        on eReadonlySnapshot do (payload: (
            reader: int,
            nextOffset: int,
            trunc: int,
            publishedEnd: int,
            segmentBase: int,
            segmentId: int,
            segmentEnd: int
        )) {
            assert payload.reader in readers &&
                !(payload.reader in lagged) &&
                payload.nextOffset == next[payload.reader],
                "readonly snapshot did not start at the follower cursor";
            snapshotEnd[payload.reader] = payload.publishedEnd;
            snapshotTrunc[payload.reader] = payload.trunc;
            snapshotSegmentBase[payload.reader] = payload.segmentBase;
            snapshotSegmentId[payload.reader] = payload.segmentId;
            snapshotSegmentEnd[payload.reader] = payload.segmentEnd;
            snapshotActive[payload.reader] = false;
        }

        on eReadonlyActiveSnapshot do (payload: (
            reader: int,
            nextOffset: int,
            trunc: int,
            segmentBase: int,
            segmentId: int
        )) {
            assert payload.reader in readers &&
                !(payload.reader in lagged) &&
                payload.nextOffset == next[payload.reader],
                "readonly active snapshot did not start at the follower cursor";
            snapshotTrunc[payload.reader] = payload.trunc;
            snapshotSegmentBase[payload.reader] = payload.segmentBase;
            snapshotSegmentId[payload.reader] = payload.segmentId;
            snapshotActive[payload.reader] = true;
        }

        on eReadonlyRecord do (payload: (
            reader: int,
            record: tRecord,
            segmentId: int
        )) {
            var key: (offset: int, value: int, segment: int);
            assert payload.reader in readers &&
                !(payload.reader in lagged) &&
                payload.record.offset == next[payload.reader],
                "readonly follower skipped or duplicated a record";
            assert payload.reader in snapshotActive &&
                payload.reader in snapshotTrunc &&
                snapshotTrunc[payload.reader] <= payload.record.offset &&
                payload.segmentId == snapshotSegmentId[payload.reader],
                "readonly follower emitted outside its manifest-selected segment";
            if (snapshotActive[payload.reader]) {
                key = RecordKey(payload.record);
                assert payload.record.segment ==
                    snapshotSegmentBase[payload.reader] &&
                    key in support && sizeof(support[key]) >= 2,
                    "readonly follower emitted an active record without majority support";
            } else {
                assert payload.record.offset in committed &&
                    committed[payload.record.offset] == payload.record,
                    "readonly follower emitted a changed sealed record";
                assert payload.reader in snapshotEnd &&
                    snapshotSegmentBase[payload.reader] <=
                        payload.record.offset &&
                    payload.record.offset <=
                        snapshotSegmentEnd[payload.reader] &&
                    snapshotSegmentEnd[payload.reader] <
                        snapshotEnd[payload.reader],
                    "readonly follower emitted outside its published sealed segment";
            }
            next[payload.reader] = payload.record.offset + 1;
        }

        on eReadonlyLagged do (payload: (
            reader: int, nextOffset: int, trunc: int
        )) {
            assert payload.reader in readers &&
                !(payload.reader in lagged) &&
                payload.nextOffset == next[payload.reader] &&
                payload.trunc > payload.nextOffset,
                "readonly follower reported lag without an overtaking floor";
            lagged += (payload.reader);
        }
    }

    fun RecordKey(record: tRecord): (
        offset: int, value: int, segment: int
    ) {
        return (offset=record.offset, value=record.value,
            segment=record.segment);
    }
}

spec GetSizeExcludesOpenTail observes eGetSizeObserved {
    start state Observing {
        on eGetSizeObserved do (payload: (
            zone: int, size: int, finalized: bool
        )) {
            assert payload.finalized || payload.size == 0,
                "GetObject.size exposed flushed unfinalized bytes";
        }
    }
}

spec ConditionalProgress observes eProgressRequested, eProgressCompleted {
    start state Idle {
        on eProgressRequested goto Waiting;
        on eProgressCompleted do {}
    }
    hot state Waiting {
        on eProgressCompleted goto Done;
        on eProgressRequested do {}
    }
    cold state Done {
        on eProgressRequested goto Waiting;
        on eProgressCompleted do {}
    }
}

// Structural facts from one atomically observed manifest snapshot. Production
// emits every directory entry in order with a snapshot index/count, so these
// checks need no cross-observation ordering assumption.
spec DirectoryStructure observes eDirectoryAdopted {
    var nextIndex: map[int, int];
    var entryCounts: map[int, int];
    var lastBases: map[int, int];
    var ids: map[int, set[int]];

    start state Observing {
        on eDirectoryAdopted do (payload: (
            writerId: int,
            directoryEntry: tDirectoryEntry,
            entryIndex: int,
            entryCount: int,
            endOffset: int,
            tailBase: int,
            currentSealBase: int,
            currentSealId: int,
            trunc: int
        )) {
            assert payload.entryCount > 0 &&
                payload.entryIndex >= 0 &&
                payload.entryIndex < payload.entryCount,
                "directory snapshot carried an invalid entry index";
            if (payload.entryIndex == 0) {
                assert !(payload.writerId in nextIndex) ||
                    nextIndex[payload.writerId] ==
                        entryCounts[payload.writerId],
                    "directory snapshot started before its predecessor ended";
                nextIndex[payload.writerId] = 0;
                entryCounts[payload.writerId] = payload.entryCount;
                ids[payload.writerId] = default(set[int]);
            }
            assert payload.writerId in nextIndex &&
                payload.entryIndex == nextIndex[payload.writerId] &&
                payload.entryCount == entryCounts[payload.writerId],
                "directory snapshot entries were incomplete or out of order";
            assert payload.directoryEntry.base < payload.tailBase,
                "directory entry reached or passed the active tail";
            assert !(payload.directoryEntry.id in ids[payload.writerId]),
                "manifest directory repeated a segment id";
            ids[payload.writerId] += (payload.directoryEntry.id);
            if (payload.entryIndex > 0) {
                assert lastBases[payload.writerId] <
                    payload.directoryEntry.base,
                    "manifest directory bases did not strictly increase";
            }
            lastBases[payload.writerId] = payload.directoryEntry.base;
            nextIndex[payload.writerId] = payload.entryIndex + 1;
            if (payload.entryIndex + 1 == payload.entryCount &&
                payload.tailBase > payload.trunc) {
                assert payload.directoryEntry.base ==
                    payload.currentSealBase &&
                    payload.directoryEntry.id == payload.currentSealId,
                    "live directory did not end with the current seal record";
            }
        }
    }
}

// Directory trust boundary. Finalized support is historical: an older adopted
// entry may have lost zones after its gate opened and still be legal to adopt
// while repair restores reachability. SealQuorumEnforced is emitted only when
// the model or production oracle first witnesses the exact finalized quorum.
// The replay/read subchecks remain model-only because production does not
// synthesize internal object reads into its trace.
//
// The finalized-quorum evidence this monitor requires is abstract. The
// implementation confirms it for a committed seal from a quorum of
// stat-reported object CRC32Cs (metadata only, matching the committed
// checksum) before adopting the seal unread, and only downloads and rewrites
// bytes when that stat quorum falls short. Both paths satisfy this monitor:
// SealQuorumEnforced witnesses the same finalized quorum either way.
spec DirectoryEnforcement observes eSealQuorumEnforced, eDirectoryAdopted,
    eDirectoryReplayed, eRead {
    var enforced: set[(segment: int, id: int, endOffset: int)];
    var adopted: set[(writerId: int, id: int)];
    var adoptedUnreadBases: set[int];

    start state Observing {
        on eSealQuorumEnforced do (payload: (
            segment: int, segmentId: int, endOffset: int
        )) {
            enforced += ((segment=payload.segment, id=payload.segmentId,
                endOffset=payload.endOffset));
        }

        on eDirectoryAdopted do (payload: (
            writerId: int,
            directoryEntry: tDirectoryEntry,
            entryIndex: int,
            entryCount: int,
            endOffset: int,
            tailBase: int,
            currentSealBase: int,
            currentSealId: int,
            trunc: int
        )) {
            var key: (segment: int, id: int, endOffset: int);
            adopted += ((writerId=payload.writerId,
                id=payload.directoryEntry.id));
            if (payload.directoryEntry.id != payload.currentSealId) {
                key = (segment=payload.directoryEntry.base,
                    id=payload.directoryEntry.id,
                    endOffset=payload.endOffset);
                assert key in enforced,
                    "directory adopted an older seal without historical finalized quorum";
                adoptedUnreadBases += (payload.directoryEntry.base);
            }
        }

        on eDirectoryReplayed do (payload: (
            writerId: int, directoryEntry: tDirectoryEntry, endOffset: int
        )) {
            assert (writerId=payload.writerId,
                id=payload.directoryEntry.id) in adopted,
                "directory replay used an entry before adoption";
        }

        on eRead do (request: tReadRequest) {
            assert !(request.segment in adoptedUnreadBases),
                "recovery reread an older adopted directory entry";
        }
    }
}

// Recovery may trust one finalized historical copy only because the manifest
// checksum identifies its exact bytes. It must copy those bytes to another
// reachable zone and restore a quorum before admitting new work.
spec HistoricalRecoveryQuorum observes eHistoricalRecoveryReady {
    start state Observing {
        on eHistoricalRecoveryReady do (payload: (
            segment: int, checksum: int, healthyZones: set[int]
        )) {
            assert sizeof(payload.healthyZones) >= 2,
                "startup completed with historical data on fewer than two zones";
        }
    }
}

// Model-only: model machines announce the decision and gate release at their
// transition points. Production can witness finalized quorums and adoption
// evidence, but its oracle cannot prove when the engine consumed the gate
// relative to a later manifest CAS, so PObserve deliberately excludes this.
spec RotationGateSafety observes eViewCommitted, eRotationGateReleased {
    var observedSealBases: set[int];
    var pendingSealBase: int;
    var hasPendingSeal: bool;

    start state Observing {
        on eViewCommitted do (payload: (
            epoch: int, tailBase: int, sealBase: int,
            sealEnd: int, sealSum: int
        )) {
            if (!(payload.sealBase in observedSealBases)) {
                assert !hasPendingSeal,
                    "next seal decision replaced a seal before its finalized quorum";
                observedSealBases += (payload.sealBase);
                pendingSealBase = payload.sealBase;
                hasPendingSeal = true;
            }
        }

        on eRotationGateReleased do (payload: (
            segment: int, segmentId: int, endOffset: int
        )) {
            if (hasPendingSeal && payload.segment == pendingSealBase) {
                hasPendingSeal = false;
            }
        }
    }
}

spec ManifestSafety observes eEpochClaimed, eViewCommitted, eFloorCommitted,
    eSegmentDeleted, eSegmentSealed, eTailRetired, eManifestCommitted,
    eDirectoryEntryRemoved {
    var owners: map[int, int];
    var maxEpoch: int;
    var tailBase: int;
    var tailGen: int;
    var sealEnds: map[int, int];
    var sealSums: map[int, int];
    var floor: int;
    var committedDirectory: seq[tDirectoryEntry];

    start state Observing {
        on eManifestCommitted do (record: tManifestRecord) {
            var index: int;
            CheckDirectory(record);
            CheckPending(record);
            if (record.directory != committedDirectory) {
                assert sizeof(record.directory) ==
                    sizeof(committedDirectory) + 1,
                    "regular manifest CAS changed the directory without one append";
                index = 0;
                while (index < sizeof(committedDirectory)) {
                    assert record.directory[index] ==
                        committedDirectory[index],
                        "new seal commit rewrote an existing directory entry";
                    index = index + 1;
                }
                assert record.directory[index].base == record.sealBase &&
                    record.directory[index].id == record.sealId,
                    "new seal commit did not append the current seal entry";
            }
            committedDirectory = record.directory;
        }

        on eDirectoryEntryRemoved do (payload: (
            directoryEntry: tDirectoryEntry,
            endOffset: int,
            floor: int,
            absentZones: set[int],
            rec: tManifestRecord
        )) {
            var index: int;
            var found: bool;
            var expected: seq[tDirectoryEntry];
            var derivedEnd: int;
            index = 0;
            found = false;
            while (index < sizeof(committedDirectory)) {
                if (committedDirectory[index] == payload.directoryEntry) {
                    found = true;
                    break;
                }
                index = index + 1;
            }
            assert found, "directory removal did not name a committed entry";
            derivedEnd = payload.rec.tailBase - 1;
            if (index + 1 < sizeof(committedDirectory)) {
                derivedEnd = committedDirectory[index + 1].base - 1;
            }
            assert payload.endOffset == derivedEnd &&
                payload.endOffset < payload.floor &&
                payload.floor <= payload.rec.trunc,
                "directory removal was not wholly below its witnessed floor";
            assert (0 in payload.absentZones) &&
                (1 in payload.absentZones) &&
                (2 in payload.absentZones),
                "directory removal lacked all-zone absence evidence";
            expected = committedDirectory;
            expected -= (index);
            assert payload.rec.directory == expected,
                "directory removal changed entries other than its tombstone";
            CheckDirectory(payload.rec);
            committedDirectory = payload.rec.directory;
        }

        on eEpochClaimed do (payload: (epoch: int, writerId: int)) {
            // the register grants each epoch exactly once. (Grant order is
            // serialized by the register; claim *observations* may arrive
            // out of order, so only uniqueness is asserted here.)
            assert !(payload.epoch in owners),
                "one epoch was granted to two incarnations";
            owners[payload.epoch] = payload.writerId;
            if (payload.epoch > maxEpoch) { maxEpoch = payload.epoch; }
        }

        on eViewCommitted do (payload: (
            epoch: int, tailBase: int, sealBase: int,
            sealEnd: int, sealSum: int
        )) {
            // commits carry a granted epoch and never regress the chain
            assert payload.epoch in owners,
                "view committed under an unclaimed epoch";
            assert payload.tailBase >= tailBase,
                "committed tail base regressed";
            assert payload.sealEnd == payload.tailBase,
                "seal end must equal the committed tail base";
            // THE dual-successor property: one seal decision per segment,
            // forever
            if (payload.sealBase in sealEnds) {
                assert sealEnds[payload.sealBase] == payload.sealEnd &&
                    sealSums[payload.sealBase] == payload.sealSum,
                    "two different seal decisions committed for one segment";
            } else {
                sealEnds[payload.sealBase] = payload.sealEnd;
                sealSums[payload.sealBase] = payload.sealSum;
            }
            tailBase = payload.tailBase;
        }

        on eTailRetired do (payload: (
            epoch: int, tailBase: int, oldGen: int, newGen: int
        )) {
            // announced by the register at the CAS apply point, so
            // observation order is commit order: retirements are strictly
            // sequential, one generation at a time, under a granted epoch
            assert payload.epoch in owners,
                "tail name retired under an unclaimed epoch";
            assert payload.oldGen == tailGen,
                "tail retirement skipped or replayed a generation";
            assert payload.newGen == payload.oldGen + 1,
                "tail retirement must advance the generation by exactly one";
            tailGen = payload.newGen;
        }

        on eFloorCommitted do (committed: int) {
            assert committed >= floor, "committed floor regressed";
            floor = committed;
        }

        on eSegmentDeleted do (payload: (
            zone: int, segment: int, endOffset: int, floor: int
        )) {
            // deletion only after the floor is committed through the register
            assert floor >= payload.floor,
                "segment deleted before its floor was committed";
        }

        on eSegmentSealed do (payload: (segment: int, endOffset: int)) {
            // finalization enforces a committed decision: the register named
            // this exact seal before any copy was finalized into a seal
            assert payload.segment in sealEnds &&
                sealEnds[payload.segment] == payload.endOffset + 1,
                "segment sealed without (or against) a committed decision";
        }
    }

    fun CheckDirectory(record: tManifestRecord) {
        var index: int;
        var ids: set[int];
        assert sizeof(record.directory) <= 4,
            "manifest directory exceeded the bounded model cap";
        index = 0;
        while (index < sizeof(record.directory)) {
            assert record.directory[index].base < record.tailBase,
                "directory entry reached or passed the active tail";
            assert !(record.directory[index].id in ids),
                "manifest directory repeated a segment id";
            ids += (record.directory[index].id);
            if (index > 0) {
                assert record.directory[index - 1].base <
                    record.directory[index].base,
                    "manifest directory bases did not strictly increase";
            }
            index = index + 1;
        }
        if (sizeof(record.directory) > 0 &&
            record.tailBase > record.trunc) {
            index = sizeof(record.directory) - 1;
            assert record.directory[index].base == record.sealBase &&
                record.directory[index].id == record.sealId,
                "live directory did not end with the current seal record";
        }
    }

    fun CheckPending(record: tManifestRecord) {
        var index: int;
        var ids: set[int];
        ids += (record.tailGen);
        index = 0;
        while (index < sizeof(record.directory)) {
            ids += (record.directory[index].id);
            index = index + 1;
        }
        if (record.pending >= 0) {
            assert !(record.pending in ids),
                "manifest pending id collided with directory or tail";
        }
    }
}
