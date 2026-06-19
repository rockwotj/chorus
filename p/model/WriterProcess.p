// One writer-process incarnation of the Chorus client.
//
// Lifecycle: claim an epoch on the manifest register (one CAS), then either
// recover-and-seal the active segment (seal-only: fence the reachable
// replicas, read them fresh, select the canonical prefix from a compatible
// pair, commit the seal decision through the manifest, write the canonical
// prefix back, finalize) or bootstrap a fresh segment and append records,
// optionally rotating (commit the rotation view, then finalize). The process
// may crash at any protocol step (bounded by crashBudget) and abdicates
// whenever the register names a higher epoch.
//
// Recovery order matters and mirrors the implementation: replicas are fenced
// (create-missing / takeover) BEFORE the reads that feed canonical
// selection, so nothing can commit between the read and the fence.
machine WriterProcess {
    var writerId: int;
    var epoch: int;
    var value: int;
    var buckets: seq[ZonalBucket];
    var manifest: ManifestRegister;
    var parent: machine;
    var shouldSeal: bool;
    var recoverExisting: bool;
    var crashBudget: int;
    var view: tManifestRecord;
    var viewMetagen: int;
    var segBase: int;
    // the object name this incarnation addresses: the manifest's tail
    // generation for the active tail and for bootstrap, or the catalog
    // wildcard (-1) when enforcing a sealed predecessor's committed name
    var gen: int;
    var lanes: set[int];
    var present: set[int];
    var missing: set[int];
    var readCopies: map[int, seq[tRecord]];
    var canonical: seq[tRecord];

    start state Run {
        entry (config: tWriterConfig) {
            var record: tRecord;
            var index: int;
            var committed: bool;
            var sealed: bool;
            writerId = config.writerId;
            value = config.value;
            buckets = config.buckets;
            segBase = config.segBase;
            manifest = config.manifest;
            parent = config.parent;
            shouldSeal = config.shouldSeal;
            recoverExisting = config.recoverExisting;
            crashBudget = config.crashBudget;
            committed = false;
            sealed = false;

            announce eProgressRequested, writerId;
            if (!Claim()) { Stop(false, false, false); return; }
            if (MaybeCrash()) { return; }

            if (recoverExisting) {
                // The committed directory is the chain authority. Recovery
                // adopts and replays every live entry without reading its
                // zonal object; only the current seal record is eligible for
                // the byte-level enforcement path below.
                AdoptDirectory();
                // The manifest, not the caller, decides what this segment is:
                // the active tail, the current seal id, an older adopted
                // directory entry, or an unknown segment. Older entries and
                // truncation tombstones are replay metadata only: do not read
                // or re-select them.
                index = DirectoryIndex(segBase);
                if (segBase > view.tailBase ||
                    (segBase < view.tailBase &&
                     (index < 0 ||
                      DirectoryEnd(index) < view.trunc ||
                      view.directory[index].id != view.sealId))) {
                    Stop(false, false, false);
                    return;
                }
                gen = view.tailGen;
                if (segBase != view.tailBase) {
                    // a sealed predecessor is addressed by its committed
                    // catalog name, not the manifest's tail id
                    gen = -1;
                }
                announce eRecoveryStarted, (
                    writerId=writerId, segment=segBase, gen=gen);
                // fence first, then read: probe reachability, revoke the
                // prior stream (or create the missing replica), and only
                // then take the reads that select the canonical prefix
                if (!ProbeReplicas()) {
                    if (segBase == view.tailBase && sizeof(missing) >= 2) {
                        // no replica of the current tail name exists on a
                        // readable quorum — but an unreachable zone may still
                        // hold bytes under this name, so the name itself is
                        // retired: commit a fresh tail generation through
                        // the register before declaring the tail empty
                        if (MaybeCrash()) { return; }
                        if (CommitRetire()) {
                            announce eProgressCompleted, writerId;
                        }
                    }
                    Stop(false, false, false);
                    return;
                }
                if (!PrepareWitnesses() || !ReadPrepared() ||
                    !SelectCanonical()) {
                    Stop(false, false, false);
                    return;
                }
                // adopt a committed seal decision: if the register already
                // names a seal for this segment, recovery enforces exactly
                // that decision — it never re-decides
                if (view.sealBase == segBase && view.sealEnd > segBase) {
                    if (sizeof(canonical) < view.sealEnd - segBase) {
                        // the committed bytes are not assembleable from the
                        // reachable replicas right now: refuse and retry later
                        Stop(false, false, false);
                        return;
                    }
                    while (sizeof(canonical) > view.sealEnd - segBase) {
                        canonical -= (sizeof(canonical) - 1);
                    }
                    if (CanonicalSum() != view.sealSum) {
                        // digest mismatch against the committed decision
                        Stop(false, false, false);
                        return;
                    }
                }
                index = 0;
                while (index < sizeof(canonical)) {
                    announce eRecoverySelected, (
                        writerId=writerId, record=canonical[index]);
                    index = index + 1;
                }
                if (sizeof(canonical) == 0) {
                    // empty tail: never reuse the name. Commit the decision
                    // (a fresh tail generation) through the register FIRST,
                    // then discard the empty fenced witnesses — decision
                    // before enforcement; the discarded objects were never
                    // acknowledged, and a crash between the two steps leaves
                    // only retired-name orphans no current-generation read
                    // can see
                    if (MaybeCrash()) { return; }
                    if (CommitRetire()) {
                        if (MaybeCrash()) { return; }
                        DiscardWitnesses();
                        announce eProgressCompleted, writerId;
                    }
                    Stop(false, false, false);
                    return;
                }
                // the seal decision: one CAS on the regional register,
                // committed before any rewrite or finalization (a no-op
                // when an already committed decision was adopted above)
                if (!CommitSeal(segBase, sizeof(canonical))) {
                    Stop(false, false, false);
                    return;
                }
                if (MaybeCrash()) { return; }
                if (!WriteBack()) { Stop(false, false, false); return; }
                announce eRecoveryCompleted, (
                    writerId=writerId, segment=segBase,
                    startOffset=segBase, endOffset=segBase + sizeof(canonical));
                if (MaybeCrash()) { return; }
                if (SealQuorum(segBase + sizeof(canonical) - 1)) {
                    announce eSealQuorumEnforced, (
                        segment=segBase, segmentId=view.sealId,
                        endOffset=segBase + sizeof(canonical) - 1);
                    announce eSegmentSealed, (
                        segment=segBase,
                        endOffset=segBase + sizeof(canonical) - 1);
                    announce eRotationGateReleased, (
                        segment=segBase, segmentId=view.sealId,
                        endOffset=segBase + sizeof(canonical) - 1);
                    sealed = true;
                }
                if (sealed) { announce eProgressCompleted, writerId; }
                Stop(false, sealed, false);
                return;
            }

            if (view.tailBase != 0) {
                // another incarnation already rotated past segment 0
                Stop(false, false, false);
                return;
            }
            gen = view.tailGen;
            if (!CreateQuorum(0)) { Stop(false, false, false); return; }
            if (MaybeCrash()) { return; }
            // creates are not epoch-guarded: revalidate ownership after
            // creating, so a creator deposed between its claim and its
            // creates abdicates here instead of acknowledging records under
            // a name the register may already have retired
            if (!RefreshView()) { Stop(false, false, false); return; }
            if (view.epoch != epoch || view.owner != writerId) {
                Stop(false, false, false);
                return;
            }
            announce eSegmentOpened, (
                segment=0, writerId=writerId, epoch=epoch, gen=gen);
            record = (offset=0, value=value, segment=0);
            announce eRecordFormed, (writerId=writerId, record=record);
            if (MaybeCrash()) { return; }
            if (!AppendQuorum(record)) { Stop(false, false, false); return; }
            canonical += (0, record);
            announce eRecordCommitted, (writerId=writerId, record=record);
            announce eRecordAcknowledged, (
                writerId=writerId, record=record, segmentId=gen);
            announce eProducerAck, (writerId=writerId, offset=record.offset);
            committed = true;

            if (shouldSeal) {
                // rotation: commit the view, then finalize the sealed segment
                if (!CommitSeal(0, sizeof(canonical))) {
                    Stop(committed, false, false);
                    return;
                }
                if (MaybeCrash()) { return; }
                if (SealQuorum(sizeof(canonical) - 1)) {
                    announce eSealQuorumEnforced, (
                        segment=0, segmentId=view.sealId,
                        endOffset=sizeof(canonical) - 1);
                    announce eSegmentSealed, (
                        segment=0, endOffset=sizeof(canonical) - 1);
                    announce eRotationGateReleased, (
                        segment=0, segmentId=view.sealId,
                        endOffset=sizeof(canonical) - 1);
                    sealed = true;
                }
            }
            announce eProgressCompleted, writerId;
            Stop(committed, sealed, false);
        }

        ignore eCreateResponse, eTakeoverResponse, eReadResponse,
            eAppendResponse, eReplaceResponse, eFinalizeResponse,
            eManifestReadResponse, eManifestCasResponse;
    }

    fun MaybeCrash(): bool {
        if (crashBudget > 0 && $) {
            crashBudget = crashBudget - 1;
            Stop(false, false, true);
            return true;
        }
        return false;
    }

    // Claim a fresh epoch with one guarded CAS; the register linearizes
    // claims, so each epoch is granted exactly once.
    fun Claim(): bool {
        var attempts: int;
        var response: tManifestCasResponse;
        var readResponse: tManifestReadResponse;
        var candidate: tManifestRecord;
        if (!RefreshView()) { return false; }
        attempts = 0;
        while (attempts < 6) {
            attempts = attempts + 1;
            candidate = (epoch=view.epoch + 1, owner=writerId,
                tailBase=view.tailBase, tailGen=view.tailGen,
                pending=view.pending,
                sealBase=view.sealBase, sealId=view.sealId,
                sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
                directory=view.directory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=viewMetagen, rec=candidate);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                response = r;
            } }
            if (response.status == STATUS_OK) {
                view = candidate;
                viewMetagen = response.metagen;
                epoch = candidate.epoch;
                announce eEpochClaimed, (epoch=epoch, writerId=writerId);
                return true;
            }
            if (!RefreshView()) { return false; }
            if (response.status == STATUS_TRANSIENT
                && view.epoch == candidate.epoch && view.owner == writerId) {
                // ambiguous CAS that applied
                epoch = view.epoch;
                viewMetagen = viewMetagen;
                announce eEpochClaimed, (epoch=epoch, writerId=writerId);
                return true;
            }
        }
        return false;
    }

    fun RefreshView(): bool {
        var attempts: int;
        var response: tManifestReadResponse;
        attempts = 0;
        while (attempts < 6) {
            attempts = attempts + 1;
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                response = r;
            } }
            if (response.status == STATUS_OK) {
                view = response.rec;
                viewMetagen = response.metagen;
                return true;
            }
        }
        return false;
    }

    fun DirectoryIndex(base: int): int {
        var index: int;
        index = 0;
        while (index < sizeof(view.directory)) {
            if (view.directory[index].base == base) { return index; }
            index = index + 1;
        }
        return -1;
    }

    fun DirectoryEnd(index: int): int {
        if (index + 1 < sizeof(view.directory)) {
            return view.directory[index + 1].base - 1;
        }
        return view.tailBase - 1;
    }

    fun AdoptDirectory() {
        var index: int;
        var endOffset: int;
        index = 0;
        while (index < sizeof(view.directory)) {
            endOffset = DirectoryEnd(index);
            announce eDirectoryAdopted, (
                writerId=writerId, directoryEntry=view.directory[index],
                entryIndex=index, entryCount=sizeof(view.directory),
                endOffset=endOffset, tailBase=view.tailBase,
                currentSealBase=view.sealBase,
                currentSealId=view.sealId, trunc=view.trunc);
            if (endOffset >= view.trunc) {
                announce eDirectoryReplayed, (
                    writerId=writerId, directoryEntry=view.directory[index],
                    endOffset=endOffset);
            }
            index = index + 1;
        }
    }

    // The digest surrogate over the canonical prefix: position-weighted sum
    // stands in for the implementation's SHA-256 (compared for equality only).
    fun CanonicalSum(): int {
        var sum: int;
        var index: int;
        sum = 0;
        index = 0;
        while (index < sizeof(canonical)) {
            sum = sum + canonical[index].value * (index + 1);
            index = index + 1;
        }
        return sum;
    }

    // Commit the seal of [segBase, segBase+count) through the register.
    // Abdicates (returns false) once a higher epoch holds the register.
    fun CommitSeal(segBase: int, count: int): bool {
        var attempts: int;
        var response: tManifestCasResponse;
        var next: tManifestRecord;
        var nextDirectory: seq[tDirectoryEntry];
        var sealId: int;
        var sum: int;
        sum = CanonicalSum();
        sealId = segBase * 10 + gen + 1;
        attempts = 0;
        while (attempts < 6) {
            attempts = attempts + 1;
            if (view.epoch != epoch || view.owner != writerId) {
                return false;   // fenced: a higher epoch claimed the register
            }
            if (view.tailBase == segBase + count && view.sealBase == segBase
                && view.sealEnd == segBase + count && view.sealSum == sum) {
                return true;    // already committed (adopted or ambiguous CAS)
            }
            nextDirectory = view.directory;
            nextDirectory += (sizeof(nextDirectory), (
                base=segBase, id=sealId));
            next = (epoch=epoch, owner=writerId, tailBase=segBase + count,
                tailGen=view.tailGen, pending=view.pending,
                sealBase=segBase, sealId=sealId,
                sealEnd=segBase + count, sealSum=sum, trunc=view.trunc,
                directory=nextDirectory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=viewMetagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                response = r;
            } }
            if (response.status == STATUS_OK) {
                view = next;
                viewMetagen = response.metagen;
                announce eViewCommitted, (
                    epoch=epoch, tailBase=next.tailBase,
                    sealBase=next.sealBase, sealEnd=next.sealEnd,
                    sealSum=next.sealSum);
                return true;
            }
            if (!RefreshView()) { return false; }
            if (view.epoch == epoch && view.owner == writerId
                && view.tailBase == segBase + count
                && view.sealBase == segBase && view.sealSum == sum) {
                announce eViewCommitted, (
                    epoch=epoch, tailBase=view.tailBase,
                    sealBase=view.sealBase, sealEnd=view.sealEnd,
                    sealSum=view.sealSum);
                return true;
            }
        }
        return false;
    }

    // Retire the current tail name: commit tailGen+1 through the register,
    // exactly the implementation's commit_view with a freshly minted
    // successor id (tail_base, seal fields, and trunc unchanged). Decision
    // before enforcement: this CAS lands before any witness is discarded.
    // Abdicates (returns false) once a higher epoch holds the register.
    fun CommitRetire(): bool {
        var attempts: int;
        var response: tManifestCasResponse;
        var next: tManifestRecord;
        var target: int;
        target = gen + 1;
        attempts = 0;
        while (attempts < 6) {
            attempts = attempts + 1;
            if (view.epoch != epoch || view.owner != writerId) {
                return false;   // fenced: a higher epoch claimed the register
            }
            if (view.tailGen == target) {
                return true;    // ambiguous CAS that applied
            }
            next = (epoch=epoch, owner=writerId, tailBase=view.tailBase,
                tailGen=target, pending=view.pending,
                sealBase=view.sealBase, sealId=view.sealId,
                sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
                directory=view.directory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=viewMetagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                response = r;
            } }
            if (response.status == STATUS_OK) {
                view = next;
                viewMetagen = response.metagen;
                return true;
            }
            if (!RefreshView()) { return false; }
        }
        return false;
    }

    // Enforcement after the decision: delete the empty fenced witnesses of
    // the retired name. Fire-and-forget — nothing depends on these deletes
    // landing; an undelivered discard leaves an orphan object whose name no
    // current-generation operation addresses.
    fun DiscardWitnesses() {
        var zone: int;
        foreach (zone in lanes) {
            send buckets[zone], eDiscardTail, (segment=segBase, gen=gen);
        }
    }

    fun CreateQuorum(segment: int): bool {
        var zone: int;
        var replies: int;
        var response: tCreateResponse;
        zone = 0;
        while (zone < sizeof(buckets)) {
            send buckets[zone], eCreateSegment, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=segment, gen=gen);
            zone = zone + 1;
        }
        replies = 0;
        while (replies < sizeof(buckets)) {
            receive { case eCreateResponse: (r: tCreateResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) { lanes += (response.zone); }
        }
        return sizeof(lanes) >= 2;
    }

    // Probe every zone to classify replicas as present or missing. The
    // copies returned here are NOT used for selection — only reachability
    // and existence; fresh reads happen after the fence.
    fun ProbeReplicas(): bool {
        var zone: int;
        var replies: int;
        var response: tReadResponse;
        zone = 0;
        while (zone < sizeof(buckets)) {
            send buckets[zone], eRead, (
                caller=this, segment=segBase, gen=gen);
            zone = zone + 1;
        }
        replies = 0;
        while (replies < sizeof(buckets)) {
            receive { case eReadResponse: (r: tReadResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) {
                present += (response.zone);
            } else if (response.status == STATUS_NOT_FOUND) {
                missing += (response.zone);
            }
        }
        return sizeof(present) + sizeof(missing) >= 2 && sizeof(present) > 0;
    }

    // Fence every reachable replica: conditionally create the missing ones
    // (an empty replica is a valid witness) and take over the streams of the
    // present ones, revoking any prior writer's handle.
    fun PrepareWitnesses(): bool {
        var zone: int;
        var expected: int;
        var replies: int;
        var prepared: set[int];
        var createResponse: tCreateResponse;
        var takeoverResponse: tTakeoverResponse;
        foreach (zone in missing) {
            send buckets[zone], eCreateSegment, (
                caller=this, writerId=writerId,
                epoch=epoch, segment=segBase, gen=gen);
        }
        foreach (zone in present) {
            send buckets[zone], eTakeoverStream, (
                caller=this, writerId=writerId,
                epoch=epoch, segment=segBase, gen=gen);
        }
        expected = sizeof(missing) + sizeof(present);
        replies = 0;
        while (replies < expected) {
            receive {
                case eCreateResponse: (r: tCreateResponse) {
                    createResponse = r;
                    if (createResponse.status == STATUS_OK) {
                        prepared += (createResponse.zone);
                    }
                }
                case eTakeoverResponse: (r: tTakeoverResponse) {
                    takeoverResponse = r;
                    if (takeoverResponse.status == STATUS_OK ||
                        takeoverResponse.status == STATUS_FINALIZED) {
                        prepared += (takeoverResponse.zone);
                    }
                }
            }
            replies = replies + 1;
        }
        lanes = prepared;
        return sizeof(lanes) >= 2;
    }

    // Fresh reads of the fenced replicas; these copies feed selection.
    fun ReadPrepared(): bool {
        var zone: int;
        var replies: int;
        var response: tReadResponse;
        foreach (zone in lanes) {
            send buckets[zone], eRead, (
                caller=this, segment=segBase, gen=gen);
        }
        replies = 0;
        while (replies < sizeof(lanes)) {
            receive { case eReadResponse: (r: tReadResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) {
                readCopies[response.zone] = response.records;
            }
        }
        return sizeof(readCopies) >= 2;
    }

    // The implementation's select_canonical_quorum: every pair of copies
    // whose overlap agrees record-for-record is a candidate, the candidate
    // canonical is the longer copy (one-witness tail promotion), and the
    // longest candidate wins. Two longest candidates that disagree mean an
    // unresolvable conflict: recovery refuses. A rotted or divergent lane is
    // tolerated exactly when two other copies form a compatible pair.
    fun SelectCanonical(): bool {
        var zones: seq[int];
        var zone: int;
        var li: int;
        var ri: int;
        var index: int;
        var left: seq[tRecord];
        var right: seq[tRecord];
        var overlap: int;
        var compatible: bool;
        var candidate: seq[tRecord];
        var haveBest: bool;
        var anyPair: bool;
        foreach (zone in keys(readCopies)) {
            zones += (sizeof(zones), zone);
        }
        haveBest = false;
        anyPair = false;
        li = 0;
        while (li < sizeof(zones)) {
            ri = li + 1;
            while (ri < sizeof(zones)) {
                left = readCopies[zones[li]];
                right = readCopies[zones[ri]];
                overlap = sizeof(left);
                if (sizeof(right) < overlap) { overlap = sizeof(right); }
                compatible = true;
                index = 0;
                while (index < overlap) {
                    if (left[index] != right[index]) { compatible = false; }
                    index = index + 1;
                }
                if (compatible) {
                    anyPair = true;
                    candidate = left;
                    if (sizeof(right) > sizeof(left)) { candidate = right; }
                    if (!haveBest || sizeof(candidate) > sizeof(canonical)) {
                        canonical = candidate;
                        haveBest = true;
                    } else if (sizeof(candidate) == sizeof(canonical)
                        && candidate != canonical) {
                        // two maximal candidates disagree: refuse
                        return false;
                    }
                }
                ri = ri + 1;
            }
            li = li + 1;
        }
        if (!anyPair) { return false; }
        index = 0;
        while (index < sizeof(canonical)) {
            if (canonical[index].offset != segBase + index) { return false; }
            index = index + 1;
        }
        return true;
    }

    fun WriteBack(): bool {
        var zone: int;
        var replies: int;
        var successes: int;
        var response: tReplaceResponse;
        foreach (zone in lanes) {
            send buckets[zone], eReplacePrefix, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=segBase, gen=gen, records=canonical);
        }
        replies = 0;
        successes = 0;
        while (replies < sizeof(lanes)) {
            receive { case eReplaceResponse: (r: tReplaceResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) { successes = successes + 1; }
            else if (response.status == STATUS_FENCED ||
                response.status == STATUS_FINALIZED) { return false; }
        }
        return successes >= 2;
    }

    fun AppendQuorum(record: tRecord): bool {
        var zone: int;
        var replies: int;
        var successes: int;
        var response: tAppendResponse;
        foreach (zone in lanes) {
            send buckets[zone], eAppend, (
                caller=this, writerId=writerId,
                epoch=epoch, gen=gen, record=record);
        }
        replies = 0;
        successes = 0;
        while (replies < sizeof(lanes)) {
            receive { case eAppendResponse: (r: tAppendResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) { successes = successes + 1; }
            else if (response.status == STATUS_FENCED ||
                response.status == STATUS_FINALIZED) { return false; }
        }
        return successes >= 2;
    }

    fun SealQuorum(validEnd: int): bool {
        var zone: int;
        var replies: int;
        var finalizedZones: set[int];
        var finalizeResponse: tFinalizeResponse;
        foreach (zone in lanes) {
            send buckets[zone], eFinalize, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=segBase, gen=gen, validEnd=validEnd);
        }
        replies = 0;
        while (replies < sizeof(lanes)) {
            receive { case eFinalizeResponse: (r: tFinalizeResponse) {
                finalizeResponse = r;
            } }
            replies = replies + 1;
            if (finalizeResponse.status == STATUS_OK) {
                finalizedZones += (finalizeResponse.zone);
            } else if (finalizeResponse.status == STATUS_FENCED ||
                finalizeResponse.status == STATUS_FINALIZED) {
                return false;
            }
        }
        return sizeof(finalizedZones) >= 2;
    }

    fun Stop(committed: bool, sealed: bool, crashed: bool) {
        send parent, eWriterDone, (
            writerId=writerId, committed=committed, sealed=sealed,
            crashed=crashed);
    }
}

// Recovery for the preregistered chain [tail, pending?]. A takeover quorum
// is established before persisted sizes are trusted, so a stale writer
// racing the new epoch either acknowledged before the recovered observation
// or loses quorum and cannot acknowledge afterward. Bases are accumulated
// from the recovered committed sizes; pending itself contains ids only.
machine PendingChainRecovery {
    var writerId: int;
    var epoch: int;
    var manifest: ManifestRegister;
    var parent: machine;
    var segmentBuckets: seq[seq[ZonalBucket]];
    var view: tManifestRecord;
    var viewMetagen: int;
    var candidateCommitted: bool;
    var candidateRecord: tRecord;
    var candidateLanes: set[int];

    start state Run {
        entry (payload: (
            writerId: int,
            manifest: ManifestRegister,
            segmentBuckets: seq[seq[ZonalBucket]],
            parent: machine
        )) {
            var ids: seq[int];
            var index: int;
            var base: int;
            var directoryEnd: int;
            writerId = payload.writerId;
            manifest = payload.manifest;
            parent = payload.parent;
            segmentBuckets = payload.segmentBuckets;

            if (!Claim()) { return; }
            if (view.sealId >= 0 &&
                view.sealBase >= 0 &&
                view.sealEnd > view.sealBase) {
                announce eRecoveryStarted, (
                    writerId=writerId,
                    segment=view.sealBase, gen=view.sealId);
                if (!FenceStatAndRead(view.sealId, view.sealBase) ||
                    !candidateCommitted) {
                    return;
                }
                announce eRecoverySelected, (
                    writerId=writerId, record=candidateRecord);
                if (!FinalizeCandidate(view.sealId, view.sealBase)) {
                    return;
                }
                announce eSealQuorumEnforced, (
                    segment=view.sealBase, segmentId=view.sealId,
                    endOffset=view.sealEnd - 1);
                announce eSegmentSealed, (
                    segment=view.sealBase,
                    endOffset=view.sealEnd - 1);
                announce eRotationGateReleased, (
                    segment=view.sealBase, segmentId=view.sealId,
                    endOffset=view.sealEnd - 1);
            }
            AdoptDirectory();
            directoryEnd = view.tailBase;
            base = view.tailBase;
            ids += (0, view.tailGen);
            if (view.pending >= 0) {
                ids += (sizeof(ids), view.pending);
            }

            index = 0;
            while (index < sizeof(ids)) {
                announce eRecoveryStarted, (
                    writerId=writerId, segment=base, gen=ids[index]);
                if (!FenceStatAndRead(ids[index], base)) { return; }
                if (!candidateCommitted) {
                    send parent, ePendingRecoveryCompleted, (
                        writerId=writerId, directoryEnd=directoryEnd,
                        recoveredEnd=base, pendingExhausted=false);
                    return;
                }
                announce eRecoverySelected, (
                    writerId=writerId, record=candidateRecord);
                if (!FinalizeCandidate(ids[index], base)) { return; }
                base = base + 1;
                index = index + 1;
            }

            // Every registered id carried committed data. Safety is intact,
            // but append admission must remain stopped until recovery or the
            // provisioner registers a fresh empty frontier.
            send parent, ePendingRecoveryCompleted, (
                writerId=writerId, directoryEnd=directoryEnd,
                recoveredEnd=base, pendingExhausted=true);
        }

        ignore eTakeoverResponse, eReadResponse, eFinalizeResponse,
            eManifestReadResponse, eManifestCasResponse;
    }

    fun Claim(): bool {
        var readResponse: tManifestReadResponse;
        var casResponse: tManifestCasResponse;
        var next: tManifestRecord;
        send manifest, eManifestRead, (caller=this,);
        receive { case eManifestReadResponse: (r: tManifestReadResponse) {
            readResponse = r;
        } }
        if (readResponse.status != STATUS_OK) { return false; }
        view = readResponse.rec;
        next = (epoch=view.epoch + 1, owner=writerId,
            tailBase=view.tailBase, tailGen=view.tailGen,
            pending=view.pending,
            sealBase=view.sealBase, sealId=view.sealId,
            sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
            directory=view.directory);
        send manifest, eManifestCas, (
            caller=this, expMetagen=readResponse.metagen, rec=next);
        receive { case eManifestCasResponse: (r: tManifestCasResponse) {
            casResponse = r;
        } }
        if (casResponse.status != STATUS_OK) { return false; }
        view = next;
        viewMetagen = casResponse.metagen;
        epoch = view.epoch;
        announce eEpochClaimed, (epoch=epoch, writerId=writerId);
        return true;
    }

    // Takeover is the fencing operation and the stat source. A potentially
    // acknowledged record needs support from Q-(N-k) of the k reachable
    // witnesses: two when all three answer, one when only a quorum answers.
    // This bounded scenario stores at most one record per candidate.
    fun FenceStatAndRead(id: int, base: int): bool {
        var zone: int;
        var replies: int;
        var nonempty: int;
        var requiredSupport: int;
        var prepared: set[int];
        var takeover: tTakeoverResponse;
        var readResponse: tReadResponse;
        var support: map[
            (offset: int, value: int, segment: int), set[int]
        ];
        var key: (offset: int, value: int, segment: int);
        var found: bool;
        candidateCommitted = false;
        candidateLanes = default(set[int]);
        zone = 0;
        while (zone < sizeof(segmentBuckets[id])) {
            send segmentBuckets[id][zone], eTakeoverStream, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=base, gen=id);
            zone = zone + 1;
        }
        replies = 0;
        nonempty = 0;
        while (replies < sizeof(segmentBuckets[id])) {
            receive { case eTakeoverResponse: (r: tTakeoverResponse) {
                takeover = r;
            } }
            replies = replies + 1;
            if (takeover.status == STATUS_OK ||
                takeover.status == STATUS_FINALIZED) {
                prepared += (takeover.zone);
                candidateLanes += (takeover.zone);
                if (takeover.persistedSize > 0) {
                    nonempty = nonempty + 1;
                }
            }
        }
        if (sizeof(prepared) < 2) { return false; }
        requiredSupport = 2 - (3 - sizeof(prepared));
        if (nonempty < requiredSupport) { return true; }

        // Read only after the takeover quorum is established. This closes the
        // claim/stat race: stale appends cannot reach a quorum after this
        // point, and quorum intersection leaves the required support for any
        // prior acknowledged record among the reachable reads.
        foreach (zone in prepared) {
            send segmentBuckets[id][zone], eRead, (
                caller=this, segment=base, gen=id);
        }
        replies = 0;
        found = false;
        while (replies < sizeof(prepared)) {
            receive { case eReadResponse: (r: tReadResponse) {
                readResponse = r;
            } }
            replies = replies + 1;
            if (readResponse.status == STATUS_OK &&
                sizeof(readResponse.records) > 0) {
                key = (offset=readResponse.records[0].offset,
                    value=readResponse.records[0].value,
                    segment=readResponse.records[0].segment);
                if (!(key in support)) {
                    support[key] = default(set[int]);
                }
                support[key] += (readResponse.zone);
                if (sizeof(support[key]) >= requiredSupport) {
                    if (found && candidateRecord != readResponse.records[0]) {
                        return false;
                    }
                    candidateRecord = readResponse.records[0];
                    found = true;
                }
            }
        }
        if (!found ||
            candidateRecord.offset != base ||
            candidateRecord.segment != base) {
            return false;
        }
        candidateCommitted = true;
        return true;
    }

    fun FinalizeCandidate(id: int, base: int): bool {
        var zone: int;
        var replies: int;
        var finalized: set[int];
        var response: tFinalizeResponse;
        foreach (zone in candidateLanes) {
            send segmentBuckets[id][zone], eFinalize, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=base, gen=id, validEnd=base);
        }
        replies = 0;
        while (replies < sizeof(candidateLanes)) {
            receive { case eFinalizeResponse: (r: tFinalizeResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK ||
                response.status == STATUS_FINALIZED) {
                finalized += (response.zone);
            }
        }
        return sizeof(finalized) >= 2;
    }

    fun DirectoryEnd(index: int): int {
        if (index + 1 < sizeof(view.directory)) {
            return view.directory[index + 1].base - 1;
        }
        return view.tailBase - 1;
    }

    fun AdoptDirectory() {
        var index: int;
        var endOffset: int;
        index = 0;
        while (index < sizeof(view.directory)) {
            endOffset = DirectoryEnd(index);
            announce eDirectoryAdopted, (
                writerId=writerId, directoryEntry=view.directory[index],
                entryIndex=index, entryCount=sizeof(view.directory),
                endOffset=endOffset, tailBase=view.tailBase,
                currentSealBase=view.sealBase,
                currentSealId=view.sealId, trunc=view.trunc);
            if (endOffset >= view.trunc) {
                announce eDirectoryReplayed, (
                    writerId=writerId, directoryEntry=view.directory[index],
                    endOffset=endOffset);
            }
            index = index + 1;
        }
    }
}
