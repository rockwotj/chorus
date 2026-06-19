// One zonal Rapid bucket's view of one segment object.
//
// `segment` is the segment's base record index; record offsets are global,
// so the local slot for offset o is o - segment.
//
// Every mutating RPC can fail ambiguously, bounded by the fault budget:
// TRANSIENT either before the operation applied (lost request) or after
// (lost response). The caller cannot distinguish the two — exactly the
// ambiguous-timeout contract of the GCS API.
//
// CRC rot is modeled as `corruptFrom`: reads decode-stop at the first
// corrupt record because the decoder does not resynchronize beyond a damaged
// envelope. Physical bytes — persisted_size, append offsets, and delete length
// checks — are unaffected. Replace and repair write fresh bytes and clear rot.
// Object identity: `gen` is the generation of the name this slot holds
// (the implementation's per-incarnation object id). One slot per bucket is
// an abstraction: creating a newer generation at the same base supersedes
// the older object, which is sound because nothing addresses a retired
// name again — the manifest names the new generation, sealed segments are
// addressed by committed catalog names (gen -1, match-any), and a zombie's
// appends to the old name can never reach a quorum (the retiring recovery
// fenced or found absent at least two of the three zones first).
machine ZonalBucket {
    var zone: int;
    var segment: int;
    var gen: int;
    var creatorWriter: int;
    var creatorEpoch: int;
    var activeWriter: int;
    var activeEpoch: int;
    var records: seq[tRecord];
    var corruptFrom: int;
    var finalized: bool;
    var objectPresent: bool;
    var up: bool;
    var failuresRemaining: int;

    start state Serving {
        entry (payload: (zone: int, failures: int)) {
            zone = payload.zone;
            segment = -1;
            gen = -1;
            creatorWriter = -1;
            creatorEpoch = 0;
            activeWriter = -1;
            activeEpoch = 0;
            corruptFrom = -1;
            finalized = false;
            objectPresent = false;
            up = true;
            failuresRemaining = payload.failures;
        }

        on eCreateSegment do HandleCreate;
        on eTakeoverStream do HandleTakeover;
        on eRead do HandleRead;
        on eAppend do HandleAppend;
        on eReplacePrefix do HandleReplace;
        on eFinalize do HandleFinalize;
        on eRepairSealed do HandleRepair;
        on eGetObject do HandleGet;
        on eDeleteSegment do HandleDelete;
        on eDiscardTail do HandleDiscard;
        on eCorruptRecord do HandleCorrupt;
        on eCrash do { up = false; }
        on eRestart do { up = true; }
    }

    // Lost request: nothing applied.
    fun Transient(): bool {
        if (!up) { return true; }
        if (failuresRemaining > 0 && $) {
            failuresRemaining = failuresRemaining - 1;
            return true;
        }
        return false;
    }

    // Applied, response lost: the ambiguous outcome.
    fun LoseResponse(): bool {
        if (failuresRemaining > 0 && $) {
            failuresRemaining = failuresRemaining - 1;
            return true;
        }
        return false;
    }

    // -1 is the catalog wildcard: sealed-segment operations address an
    // exact committed name in the implementation, so they always match.
    fun GenMatches(reqGen: int): bool {
        return reqGen == -1 || reqGen == gen;
    }

    fun HandleCreate(req: tCreateRequest) {
        if (Transient()) {
            send req.caller, eCreateResponse,
                (zone=zone, status=STATUS_TRANSIENT);
        } else if (objectPresent && (req.gen == gen || req.gen == -1)) {
            send req.caller, eCreateResponse,
                (zone=zone, status=STATUS_EXISTS);
        } else if (objectPresent && req.gen < gen) {
            // a stale creator addressing a retired name: in the real bucket
            // the create lands on an orphan object nobody will ever read;
            // FENCED is the sound one-slot rendering — the creator can never
            // assemble a quorum under a retired name either way
            send req.caller, eCreateResponse,
                (zone=zone, status=STATUS_FENCED);
        } else {
            // absent, or this slot holds an older generation: the requested
            // name does not exist yet; create it (superseding the retired
            // object, whose bytes were never acknowledged)
            assert !(objectPresent && finalized),
                "a newer tail generation superseded a sealed object";
            objectPresent = true;
            segment = req.segment;
            gen = req.gen;
            creatorWriter = req.writerId;
            creatorEpoch = req.epoch;
            activeWriter = req.writerId;
            activeEpoch = req.epoch;
            records = default(seq[tRecord]);
            corruptFrom = -1;
            finalized = false;
            announce eSegmentCreated, (
                zone=zone, writerId=req.writerId,
                epoch=req.epoch, segment=req.segment, gen=req.gen);
            if (LoseResponse()) {
                send req.caller, eCreateResponse,
                    (zone=zone, status=STATUS_TRANSIENT);
            } else {
                send req.caller, eCreateResponse,
                    (zone=zone, status=STATUS_OK);
            }
        }
    }

    fun HandleTakeover(req: tTakeoverRequest) {
        if (Transient()) {
            send req.caller, eTakeoverResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                persistedSize=sizeof(records));
        } else if (!objectPresent || segment != req.segment ||
            !GenMatches(req.gen)) {
            send req.caller, eTakeoverResponse, (
                zone=zone, status=STATUS_NOT_FOUND,
                persistedSize=sizeof(records));
        } else if (finalized) {
            send req.caller, eTakeoverResponse, (
                zone=zone, status=STATUS_FINALIZED,
                persistedSize=sizeof(records));
        } else {
            activeWriter = req.writerId;
            activeEpoch = req.epoch;
            announce eStreamTakenOver, (
                zone=zone, writerId=req.writerId,
                epoch=req.epoch, segment=req.segment);
            if (LoseResponse()) {
                send req.caller, eTakeoverResponse, (
                    zone=zone, status=STATUS_TRANSIENT,
                    persistedSize=sizeof(records));
            } else {
                send req.caller, eTakeoverResponse, (
                    zone=zone, status=STATUS_OK,
                    persistedSize=sizeof(records));
            }
        }
    }

    // Reads return the decodable prefix: everything before the first
    // corrupt record. The reader does not resynchronize beyond rot, so a
    // rotted lane is indistinguishable from a short one.
    fun DecodablePrefix(): seq[tRecord] {
        var prefix: seq[tRecord];
        var index: int;
        if (corruptFrom < 0) { return records; }
        index = 0;
        while (index < corruptFrom) {
            prefix += (index, records[index]);
            index = index + 1;
        }
        return prefix;
    }

    fun HandleRead(req: tReadRequest) {
        var empty: seq[tRecord];
        if (Transient()) {
            send req.caller, eReadResponse, (
                zone=zone, status=STATUS_TRANSIENT, records=empty,
                finalized=finalized);
        } else if (!objectPresent || segment != req.segment ||
            !GenMatches(req.gen)) {
            send req.caller, eReadResponse, (
                zone=zone, status=STATUS_NOT_FOUND, records=empty,
                finalized=false);
        } else {
            send req.caller, eReadResponse, (
                zone=zone, status=STATUS_OK, records=DecodablePrefix(),
                finalized=finalized);
        }
    }

    fun HandleAppend(req: tAppendRequest) {
        var current: tRecord;
        var slot: int;
        if (Transient()) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                persistedSize=sizeof(records), offset=req.record.offset);
            return;
        }
        if (!objectPresent || req.record.segment != segment ||
            !GenMatches(req.gen)) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_FENCED,
                persistedSize=sizeof(records), offset=req.record.offset);
            return;
        }
        if (finalized) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_FINALIZED,
                persistedSize=sizeof(records), offset=req.record.offset);
            return;
        }
        if (req.writerId != activeWriter || req.epoch != activeEpoch) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_FENCED,
                persistedSize=sizeof(records), offset=req.record.offset);
            return;
        }
        slot = req.record.offset - segment;
        if (slot >= 0 && slot < sizeof(records)) {
            current = records[slot];
            if (current == req.record) {
                send req.caller, eAppendResponse, (
                    zone=zone, status=STATUS_OK,
                    persistedSize=sizeof(records), offset=req.record.offset);
            } else {
                send req.caller, eAppendResponse, (
                    zone=zone, status=STATUS_FENCED,
                    persistedSize=sizeof(records), offset=req.record.offset);
            }
            return;
        }
        if (slot != sizeof(records)) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_FENCED,
                persistedSize=sizeof(records), offset=req.record.offset);
            return;
        }
        records += (sizeof(records), req.record);
        announce eRecordPersisted, (
            zone=zone, writerId=req.writerId, record=req.record, gen=gen);
        if (LoseResponse()) {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                persistedSize=sizeof(records), offset=req.record.offset);
        } else {
            send req.caller, eAppendResponse, (
                zone=zone, status=STATUS_OK,
                persistedSize=sizeof(records), offset=req.record.offset);
        }
    }

    fun HandleReplace(req: tReplaceRequest) {
        var index: int;
        if (Transient()) {
            send req.caller, eReplaceResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                persistedSize=sizeof(records));
            return;
        }
        if (!objectPresent || segment != req.segment ||
            !GenMatches(req.gen) ||
            (!finalized &&
             (req.writerId != activeWriter || req.epoch != activeEpoch))) {
            send req.caller, eReplaceResponse, (
                zone=zone, status=STATUS_FENCED,
                persistedSize=sizeof(records));
            return;
        }
        activeWriter = req.writerId;
        activeEpoch = req.epoch;
        finalized = false;
        records = req.records;
        corruptFrom = -1;
        index = 0;
        while (index < sizeof(records)) {
            announce eCanonicalPersisted, (
                zone=zone, writerId=req.writerId, record=records[index]);
            index = index + 1;
        }
        if (LoseResponse()) {
            send req.caller, eReplaceResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                persistedSize=sizeof(records));
        } else {
            send req.caller, eReplaceResponse, (
                zone=zone, status=STATUS_OK,
                persistedSize=sizeof(records));
        }
    }

    fun HandleFinalize(req: tFinalizeRequest) {
        if (Transient()) {
            send req.caller, eFinalizeResponse, (
                zone=zone, status=STATUS_TRANSIENT, validEnd=req.validEnd);
        } else if (!objectPresent || segment != req.segment ||
            !GenMatches(req.gen)) {
            send req.caller, eFinalizeResponse, (
                zone=zone, status=STATUS_FENCED, validEnd=req.validEnd);
        } else if (finalized) {
            send req.caller, eFinalizeResponse, (
                zone=zone, status=STATUS_FINALIZED, validEnd=req.validEnd);
        } else if (req.writerId != activeWriter || req.epoch != activeEpoch) {
            send req.caller, eFinalizeResponse, (
                zone=zone, status=STATUS_FENCED, validEnd=req.validEnd);
        } else if (req.validEnd - segment < 0 ||
            req.validEnd - segment >= sizeof(records)) {
            send req.caller, eFinalizeResponse, (
                zone=zone, status=STATUS_FENCED, validEnd=req.validEnd);
        } else {
            finalized = true;
            announce eSegmentFinalized, (
                zone=zone, segment=segment, validEnd=req.validEnd);
            if (LoseResponse()) {
                send req.caller, eFinalizeResponse, (
                    zone=zone, status=STATUS_TRANSIENT, validEnd=req.validEnd);
            } else {
                send req.caller, eFinalizeResponse, (
                    zone=zone, status=STATUS_OK, validEnd=req.validEnd);
            }
        }
    }

    fun HandleRepair(req: tRepairRequest) {
        if (Transient()) {
            send req.caller, eRepairResponse,
                (zone=zone, status=STATUS_TRANSIENT);
        } else if (req.validEnd - req.segment < 0 ||
            req.validEnd - req.segment + 1 != sizeof(req.records)) {
            send req.caller, eRepairResponse,
                (zone=zone, status=STATUS_FENCED);
        } else if (objectPresent && !finalized) {
            // Repair is forbidden for the active appendable object.
            send req.caller, eRepairResponse,
                (zone=zone, status=STATUS_FENCED);
        } else {
            objectPresent = true;
            segment = req.segment;
            records = req.records;
            corruptFrom = -1;
            finalized = true;
            announce eSegmentFinalized, (
                zone=zone, segment=segment, validEnd=req.validEnd);
            announce eSealedCopyRepaired, (
                zone=zone, segment=segment, endOffset=req.validEnd,
                recordCount=sizeof(records));
            send req.caller, eRepairResponse,
                (zone=zone, status=STATUS_OK);
        }
    }

    fun HandleGet(req: tGetRequest) {
        var reported: int;
        if (Transient()) {
            send req.caller, eGetResponse, (
                zone=zone, status=STATUS_TRANSIENT,
                reportedSize=0, finalized=finalized);
            return;
        }
        reported = 0;
        if (finalized) { reported = sizeof(records); }
        announce eGetSizeObserved, (
            zone=zone, size=reported, finalized=finalized);
        send req.caller, eGetResponse, (
            zone=zone, status=STATUS_OK,
            reportedSize=reported, finalized=finalized);
    }

    fun HandleDelete(req: tDeleteRequest) {
        if (Transient()) {
            send req.caller, eDeleteResponse, (
                zone=zone, status=STATUS_TRANSIENT, segment=req.segment);
            return;
        }
        if (objectPresent && finalized && segment == req.segment &&
            req.endOffset - segment + 1 == sizeof(records) &&
            req.endOffset < req.floor) {
            objectPresent = false;
            announce eSegmentDeleted, (
                zone=zone, segment=req.segment,
                endOffset=req.endOffset, floor=req.floor);
        }
        if (LoseResponse()) {
            send req.caller, eDeleteResponse, (
                zone=zone, status=STATUS_TRANSIENT, segment=req.segment);
        } else {
            send req.caller, eDeleteResponse, (
                zone=zone, status=STATUS_OK, segment=req.segment);
        }
    }

    // Discard a retired tail name (fire-and-forget). Guarded by the exact
    // generation: the recoverer commits the fresh generation through the
    // register first, so a discard arriving after a newer create at the
    // same base is a no-op on the new object — deletes address one name.
    fun HandleDiscard(req: tDiscardRequest) {
        if (Transient()) { return; }
        if (objectPresent && segment == req.segment && gen == req.gen) {
            assert !finalized,
                "discard of a retired tail name reached a sealed object";
            objectPresent = false;
        }
    }

    // Fault injection, not an RPC: rot the stored record at a global
    // offset. Decoders stop there; bytes and sizes are unchanged.
    fun HandleCorrupt(req: (offset: int)) {
        var slot: int;
        slot = req.offset - segment;
        if (objectPresent && slot >= 0 && slot < sizeof(records)) {
            if (corruptFrom < 0 || slot < corruptFrom) {
                corruptFrom = slot;
            }
        }
    }
}
