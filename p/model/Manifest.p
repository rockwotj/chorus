// The regional manifest register.
//
// One object in a regional GCS bucket, mutated only by generation/
// metageneration-guarded UpdateObject. GCS strong consistency makes it a
// linearizable CAS register; the provider's regional replication makes it
// zone-fault tolerant. Handler atomicity models exactly that trusted
// linearizability — the model does not re-derive it, mirroring how takeover
// revocation is trusted service behavior.
//
// Every mutating request can fail ambiguously: TRANSIENT either before the
// CAS applied (lost request) or after (lost response), bounded by a fault
// budget. STATUS_FENCED models FAILED_PRECONDITION on a guard mismatch.
machine ManifestRegister {
    var metagen: int;
    var rec: tManifestRecord;
    var faultBudget: int;

    start state Serving {
        entry (config: (failures: int)) {
            metagen = 1;
            rec = (epoch=0, owner=0, tailBase=0, tailGen=0,
                pending=-1, sealBase=-1,
                sealId=-1, sealEnd=0, sealSum=0, trunc=0,
                directory=default(seq[tDirectoryEntry]));
            faultBudget = config.failures;
        }

        on eManifestRead do (request: tManifestReadRequest) {
            if (Flaky()) {
                send request.caller, eManifestReadResponse, (
                    status=STATUS_TRANSIENT, metagen=0, rec=rec);
                return;
            }
            send request.caller, eManifestReadResponse, (
                status=STATUS_OK, metagen=metagen, rec=rec);
        }

        on eManifestCas do (request: tManifestCasRequest) {
            if (Flaky()) {
                // lost request: nothing applied
                send request.caller, eManifestCasResponse, (
                    status=STATUS_TRANSIENT, metagen=0, rec=rec);
                return;
            }
            if (request.expMetagen != metagen) {
                send request.caller, eManifestCasResponse, (
                    status=STATUS_FENCED, metagen=metagen, rec=rec);
                return;
            }
            if (!ValidCasDirectory(request.rec) ||
                !ValidPendingTransition(request.rec)) {
                send request.caller, eManifestCasResponse, (
                    status=STATUS_FENCED, metagen=metagen, rec=rec);
                return;
            }
            if (request.rec.tailBase == rec.tailBase &&
                request.rec.tailGen > rec.tailGen) {
                // a tail name is being retired: announce at the
                // linearization point, before the response can race
                // any reader of the new generation
                announce eTailRetired, (
                    epoch=request.rec.epoch, tailBase=rec.tailBase,
                    oldGen=rec.tailGen, newGen=request.rec.tailGen);
            }
            rec = request.rec;
            metagen = metagen + 1;
            announce eManifestCommitted, rec;
            if (Flaky()) {
                // applied, response lost: the ambiguous outcome
                send request.caller, eManifestCasResponse, (
                    status=STATUS_TRANSIENT, metagen=0, rec=rec);
                return;
            }
            send request.caller, eManifestCasResponse, (
                status=STATUS_OK, metagen=metagen, rec=rec);
        }

        on eDirectoryRemove do HandleDirectoryRemove;
    }

    // Regular register CASes either preserve the directory exactly or append
    // one new current-seal entry. Cleanup uses the separate evidence-carrying
    // removal transition below.
    fun ValidCasDirectory(next: tManifestRecord): bool {
        var index: int;
        if (sizeof(next.directory) > 4) { return false; }
        if (next.sealId == rec.sealId) {
            return next.directory == rec.directory &&
                next.sealBase == rec.sealBase &&
                next.sealEnd == rec.sealEnd &&
                next.sealSum == rec.sealSum;
        }
        if (next.sealId < 0 ||
            sizeof(next.directory) != sizeof(rec.directory) + 1) {
            return false;
        }
        index = 0;
        while (index < sizeof(rec.directory)) {
            if (next.directory[index] != rec.directory[index]) {
                return false;
            }
            index = index + 1;
        }
        return next.directory[index].base == next.sealBase &&
            next.directory[index].id == next.sealId;
    }

    // One pending id may be registered while the tail is stable. A maintenance
    // fold consumes it as the new tail and may atomically install one already
    // provisioned refill id.
    fun ValidPendingTransition(next: tManifestRecord): bool {
        if (!ValidIdentitySet(next)) { return false; }
        if (next.pending == rec.pending) { return true; }

        // Off-path preregistration: fill an empty pending slot.
        if (next.tailBase == rec.tailBase &&
            next.tailGen == rec.tailGen &&
            rec.pending < 0 &&
            next.pending >= 0) {
            return true;
        }

        // Off-path maintenance: fold the old tail into the directory and
        // advance to the preregistered id. Pending is either cleared or
        // replaced with one already provisioned fresh id.
        if (rec.pending < 0 ||
            next.tailGen != rec.pending ||
            next.tailBase <= rec.tailBase ||
            next.sealBase != rec.tailBase ||
            next.sealId != rec.tailGen) {
            return false;
        }
        return true;
    }

    fun ValidIdentitySet(record: tManifestRecord): bool {
        var index: int;
        var ids: set[int];
        index = 0;
        while (index < sizeof(record.directory)) {
            ids += (record.directory[index].id);
            index = index + 1;
        }
        if (record.tailGen in ids) { return false; }
        ids += (record.tailGen);
        if (record.pending >= 0 && record.pending in ids) {
            return false;
        }
        return true;
    }

    fun HandleDirectoryRemove(request: tDirectoryRemoveRequest) {
        var index: int;
        var found: bool;
        var endOffset: int;
        var next: tManifestRecord;
        if (Flaky()) {
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_TRANSIENT, metagen=0, rec=rec);
            return;
        }
        if (request.expMetagen != metagen) {
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_FENCED, metagen=metagen, rec=rec);
            return;
        }
        if (request.floor > rec.trunc ||
            !(0 in request.absentZones) ||
            !(1 in request.absentZones) ||
            !(2 in request.absentZones)) {
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_FENCED, metagen=metagen, rec=rec);
            return;
        }
        index = 0;
        found = false;
        while (index < sizeof(rec.directory)) {
            if (rec.directory[index] == request.directoryEntry) {
                found = true;
                break;
            }
            index = index + 1;
        }
        if (!found) {
            // A racing or ambiguously successful cleanup already removed it.
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_OK, metagen=metagen, rec=rec);
            return;
        }
        endOffset = rec.tailBase - 1;
        if (index + 1 < sizeof(rec.directory)) {
            endOffset = rec.directory[index + 1].base - 1;
        }
        if (endOffset >= request.floor) {
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_FENCED, metagen=metagen, rec=rec);
            return;
        }
        next = rec;
        next.directory -= (index);
        rec = next;
        metagen = metagen + 1;
        announce eDirectoryEntryRemoved, (
            directoryEntry=request.directoryEntry, endOffset=endOffset,
            floor=request.floor,
            absentZones=request.absentZones, rec=rec);
        if (Flaky()) {
            send request.caller, eDirectoryRemoveResponse, (
                status=STATUS_TRANSIENT, metagen=0, rec=rec);
            return;
        }
        send request.caller, eDirectoryRemoveResponse, (
            status=STATUS_OK, metagen=metagen, rec=rec);
    }

    fun Flaky(): bool {
        if (faultBudget > 0 && $) {
            faultBudget = faultBudget - 1;
            return true;
        }
        return false;
    }
}
