machine PipelinedWriter {
    var epoch: int;
    var viewMetagen: int;
    var view: tManifestRecord;
    var manifest: ManifestRegister;

    start state Run {
        entry (payload: (
            writerId: int, manifest: ManifestRegister,
            buckets: seq[ZonalBucket], nextBuckets: seq[ZonalBucket],
            parent: machine
        )) {
            var zone: int;
            var replies: int;
            var lanes: set[int];
            var finalized: set[int];
            var nextFinalized: set[int];
            var created: tCreateResponse;
            var appended: tAppendResponse;
            var finalizedResponse: tFinalizeResponse;
            var casResponse: tManifestCasResponse;
            var readResponse: tManifestReadResponse;
            var next: tManifestRecord;
            var nextDirectory: seq[tDirectoryEntry];
            var record0: tRecord;
            var record1: tRecord;
            var record2: tRecord;
            var support0: int;
            var support1: int;
            var spareLanes: set[int];
            var spareSupport: int;

            manifest = payload.manifest;
            // claim a fresh epoch: one CAS on the regional register
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            view = readResponse.rec;
            viewMetagen = readResponse.metagen;
            next = (epoch=view.epoch + 1, owner=payload.writerId,
                tailBase=view.tailBase, tailGen=view.tailGen,
                pending=view.pending,
                sealBase=view.sealBase, sealId=view.sealId,
                sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
                directory=view.directory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=viewMetagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            if (casResponse.status != STATUS_OK) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            view = next;
            viewMetagen = casResponse.metagen;
            epoch = next.epoch;
            announce eEpochClaimed, (epoch=epoch, writerId=payload.writerId);

            zone = 0;
            while (zone < sizeof(payload.buckets)) {
                send payload.buckets[zone], eCreateSegment, (
                    caller=this, writerId=payload.writerId,
                    epoch=epoch, segment=0, gen=view.tailGen);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < sizeof(payload.buckets)) {
                receive { case eCreateResponse: (r: tCreateResponse) {
                    created = r;
                } }
                replies = replies + 1;
                if (created.status == STATUS_OK) { lanes += (created.zone); }
            }
            if (sizeof(lanes) < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            announce eSegmentOpened, (
                segment=0, writerId=payload.writerId, epoch=epoch,
                gen=view.tailGen);

            record0 = (offset=0, value=100, segment=0);
            record1 = (offset=1, value=200, segment=0);
            announce eRecordFormed, (
                writerId=payload.writerId, record=record0);
            announce eRecordFormed, (
                writerId=payload.writerId, record=record1);

            // Dispatch the whole bounded window before observing replies.
            foreach (zone in lanes) {
                send payload.buckets[zone], eAppend, (
                    caller=this, writerId=payload.writerId,
                    epoch=epoch, gen=view.tailGen, record=record0);
                send payload.buckets[zone], eAppend, (
                    caller=this, writerId=payload.writerId,
                    epoch=epoch, gen=view.tailGen, record=record1);
            }
            replies = 0;
            support0 = 0;
            support1 = 0;
            while (replies < sizeof(lanes) * 2) {
                receive { case eAppendResponse: (r: tAppendResponse) {
                    appended = r;
                } }
                replies = replies + 1;
                if (appended.status == STATUS_OK) {
                    if (appended.offset == 0) { support0 = support0 + 1; }
                    else if (appended.offset == 1) {
                        support1 = support1 + 1;
                    }
                }
            }
            if (support0 < 2 || support1 < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }

            announce eRecordCommitted, (
                writerId=payload.writerId, record=record0);
            announce eRecordAcknowledged, (
                writerId=payload.writerId, record=record0,
                segmentId=view.tailGen);
            announce eProducerAck, (
                writerId=payload.writerId, offset=record0.offset);
            announce eRecordCommitted, (
                writerId=payload.writerId, record=record1);
            announce eRecordAcknowledged, (
                writerId=payload.writerId, record=record1,
                segmentId=view.tailGen);
            announce eProducerAck, (
                writerId=payload.writerId, offset=record1.offset);

            // Deferred-swap rotation: with the old segment at its committed
            // boundary (admission stopped, every admitted record committed
            // and acknowledged), provision the spare successor and admit
            // into it BEFORE the rotation CAS. Acknowledgments for successor
            // records gate on the CAS landing: until the manifest names the
            // spare, its bytes are unnamed orphans recovery rightly ignores.
            spareSupport = 0;
            if (sizeof(payload.nextBuckets) > 0) {
                zone = 0;
                while (zone < sizeof(payload.nextBuckets)) {
                    send payload.nextBuckets[zone], eCreateSegment, (
                        caller=this, writerId=payload.writerId,
                        epoch=epoch, segment=2, gen=view.tailGen);
                    zone = zone + 1;
                }
                replies = 0;
                while (replies < sizeof(payload.nextBuckets)) {
                    receive { case eCreateResponse: (r: tCreateResponse) {
                        created = r;
                    } }
                    replies = replies + 1;
                    if (created.status == STATUS_OK) {
                        spareLanes += (created.zone);
                    }
                }
                if (sizeof(spareLanes) >= 2) {
                    announce eSegmentOpened, (
                        segment=2, writerId=payload.writerId, epoch=epoch,
                        gen=view.tailGen);
                    record2 = (offset=2, value=300, segment=2);
                    announce eRecordFormed, (
                        writerId=payload.writerId, record=record2);
                    foreach (zone in spareLanes) {
                        send payload.nextBuckets[zone], eAppend, (
                            caller=this, writerId=payload.writerId,
                            epoch=epoch, gen=view.tailGen, record=record2);
                    }
                    replies = 0;
                    while (replies < sizeof(spareLanes)) {
                        receive { case eAppendResponse: (r: tAppendResponse) {
                            appended = r;
                        } }
                        replies = replies + 1;
                        if (appended.status == STATUS_OK) {
                            spareSupport = spareSupport + 1;
                        }
                    }
                }
                // the vulnerable window: successor bytes are durable on the
                // zones but the manifest still names segment 0 as the open
                // tail and no successor acknowledgment was issued; a crash
                // here must leave the log recoverable at segment 0's
                // committed boundary
                if ($) {
                    send payload.parent, eWriterDone, (
                        writerId=payload.writerId, committed=true,
                        sealed=false, crashed=true);
                    return;
                }
            }

            // the rotation CAS: seal segment 0 at the committed boundary and
            // name the successor as the new tail in one manifest commit
            nextDirectory = view.directory;
            nextDirectory += (sizeof(nextDirectory), (base=0, id=1));
            next = (epoch=epoch, owner=payload.writerId, tailBase=2,
                tailGen=view.tailGen, pending=view.pending,
                sealBase=0, sealId=1, sealEnd=2,
                sealSum=record0.value * 1 + record1.value * 2,
                trunc=view.trunc, directory=nextDirectory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=viewMetagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            if (casResponse.status != STATUS_OK) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }
            view = next;
            viewMetagen = casResponse.metagen;
            announce eViewCommitted, (
                epoch=epoch, tailBase=next.tailBase, sealBase=next.sealBase,
                sealEnd=next.sealEnd, sealSum=next.sealSum);
            // the gated acknowledgment lifts only now that the manifest
            // names the successor as the open tail
            if (spareSupport >= 2) {
                announce eRecordCommitted, (
                    writerId=payload.writerId, record=record2);
                announce eRecordAcknowledged, (
                    writerId=payload.writerId, record=record2,
                    segmentId=view.tailGen);
                announce eProducerAck, (
                    writerId=payload.writerId, offset=record2.offset);
            }
            // The production rotation gate remains closed after the decision
            // CAS until the exact sealed segment reaches a finalized quorum.
            // Model both a process crash and maintenance that stalls/fails in
            // this window: neither path may attempt the next rotation.
            if (sizeof(payload.nextBuckets) > 0 && $) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=true);
                return;
            }
            if (sizeof(payload.nextBuckets) > 0 && $) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }
            // decision-then-enforcement: finalize the sealed segment's
            // lanes only after the manifest decision is committed
            foreach (zone in lanes) {
                send payload.buckets[zone], eFinalize, (
                    caller=this, writerId=payload.writerId,
                    epoch=epoch, segment=0, gen=view.tailGen, validEnd=1);
            }
            replies = 0;
            while (replies < sizeof(lanes)) {
                receive { case eFinalizeResponse: (r: tFinalizeResponse) {
                    finalizedResponse = r;
                } }
                replies = replies + 1;
                if (finalizedResponse.status == STATUS_OK) {
                    finalized += (finalizedResponse.zone);
                }
            }
            if (sizeof(finalized) < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }
            announce eSealQuorumEnforced, (
                segment=0, segmentId=1, endOffset=1);
            announce eSegmentSealed, (segment=0, endOffset=1);
            announce eRotationGateReleased, (
                segment=0, segmentId=1, endOffset=1);

            // The next swap is admitted only after the exact finalized quorum
            // emits eRotationGateReleased. Committing this second decision is
            // the schedule that makes a broken gate observable.
            if (spareSupport >= 2) {
                nextDirectory = view.directory;
                nextDirectory += (sizeof(nextDirectory), (base=2, id=21));
                next = (epoch=epoch, owner=payload.writerId, tailBase=3,
                    tailGen=view.tailGen, pending=view.pending,
                    sealBase=2, sealId=21, sealEnd=3,
                    sealSum=record2.value, trunc=view.trunc,
                    directory=nextDirectory);
                send manifest, eManifestCas, (
                    caller=this, expMetagen=viewMetagen, rec=next);
                receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                    casResponse = r;
                } }
                if (casResponse.status != STATUS_OK) {
                    send payload.parent, eWriterDone, (
                        writerId=payload.writerId, committed=true,
                        sealed=false, crashed=false);
                    return;
                }
                view = next;
                viewMetagen = casResponse.metagen;
                announce eViewCommitted, (
                    epoch=epoch, tailBase=next.tailBase,
                    sealBase=next.sealBase, sealEnd=next.sealEnd,
                    sealSum=next.sealSum);

                foreach (zone in spareLanes) {
                    send payload.nextBuckets[zone], eFinalize, (
                        caller=this, writerId=payload.writerId,
                        epoch=epoch, segment=2, gen=view.tailGen, validEnd=2);
                }
                replies = 0;
                while (replies < sizeof(spareLanes)) {
                    receive { case eFinalizeResponse: (r: tFinalizeResponse) {
                        finalizedResponse = r;
                    } }
                    replies = replies + 1;
                    if (finalizedResponse.status == STATUS_OK) {
                        nextFinalized += (finalizedResponse.zone);
                    }
                }
                if (sizeof(nextFinalized) < 2) {
                    send payload.parent, eWriterDone, (
                        writerId=payload.writerId, committed=true,
                        sealed=false, crashed=false);
                    return;
                }
                announce eSealQuorumEnforced, (
                    segment=2, segmentId=21, endOffset=2);
                announce eSegmentSealed, (segment=2, endOffset=2);
                announce eRotationGateReleased, (
                    segment=2, segmentId=21, endOffset=2);
            }
            send payload.parent, eWriterDone, (
                writerId=payload.writerId, committed=true,
                sealed=true, crashed=false);
        }

        ignore eCreateResponse, eAppendResponse, eFinalizeResponse,
            eManifestReadResponse, eManifestCasResponse;
    }
}

// Single-pending rotation model. Provisioning and preregistration happen
// before the rotation boundary. Once an id is in manifest.pending, activation
// is an in-memory lane flip: no create, stream open, or manifest CAS occurs
// between the old segment's final acknowledgment and the successor's first
// append.
machine PendingSegmentWriter {
    var writerId: int;
    var epoch: int;
    var manifest: ManifestRegister;
    var parent: machine;
    var segmentBuckets: seq[seq[ZonalBucket]];
    var view: tManifestRecord;
    var viewMetagen: int;

    start state Run {
        entry (payload: (
            writerId: int,
            manifest: ManifestRegister,
            segmentBuckets: seq[seq[ZonalBucket]],
            pauseBeforeRotation: bool,
            landMaintenance: bool,
            attemptSecondRotation: bool,
            attemptGapRotation: bool,
            parent: machine
        )) {
            var record: tRecord;
            var committedEnd: int;
            var maintenanceLanded: bool;
            var exhaustedAfterRotation: bool;
            writerId = payload.writerId;
            manifest = payload.manifest;
            parent = payload.parent;
            segmentBuckets = payload.segmentBuckets;
            committedEnd = 0;
            maintenanceLanded = false;
            exhaustedAfterRotation = false;

            if (!Claim() || !CreateSegment(0, 0)) {
                Done(committedEnd, false, false);
                return;
            }
            record = (offset=0, value=400, segment=0);
            if (!AppendAndAck(0, record)) {
                Done(committedEnd, false, false);
                return;
            }
            committedEnd = 1;

            if (!CreateSegment(2, 1) || !RegisterPending(2)) {
                Done(committedEnd, false, true);
                return;
            }

            if (payload.pauseBeforeRotation) {
                send parent, ePendingWriterReady;
                receive { case ePendingWriterContinue: {} }
            }

            // The production gate must not derive the successor base from
            // admitted records. Put offset 1 on only one old-tail replica and
            // stop before the flip: the record is durable but uncommitted, so
            // no pending record may be admitted above it.
            if (payload.attemptGapRotation) {
                record = (offset=1, value=450, segment=0);
                if (!AppendWithoutQuorum(0, 0, record)) {
                    Done(committedEnd, false, false);
                    return;
                }
                Done(committedEnd, false, false);
                return;
            }

            // CAS-free flip into the one pending segment.
            record = (offset=1, value=500, segment=1);
            if (!AppendAndAck(2, record)) {
                Done(committedEnd, false, false);
                return;
            }
            committedEnd = 2;

            // The in-memory spare is now consumed. A second rotation before
            // fold/refill must apply backpressure rather than append to an
            // unregistered object.
            if (payload.attemptSecondRotation) {
                exhaustedAfterRotation = true;
            }

            // The process may die here: pending already contains
            // acknowledged data, while the manifest still names segment 0 as
            // tail. If maintenance lands, it first provisions a fresh empty
            // spare, then one CAS folds the old tail and installs the refill.
            if (payload.landMaintenance) {
                if (CreateSegment(3, 2)) {
                    maintenanceLanded = FoldTail(3);
                }
            }
            Done(committedEnd, maintenanceLanded,
                exhaustedAfterRotation);
        }

        ignore eCreateResponse, eAppendResponse, eManifestReadResponse,
            eManifestCasResponse;
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

    fun CreateSegment(id: int, base: int): bool {
        var zone: int;
        var replies: int;
        var successes: int;
        var response: tCreateResponse;
        zone = 0;
        while (zone < sizeof(segmentBuckets[id])) {
            send segmentBuckets[id][zone], eCreateSegment, (
                caller=this, writerId=writerId, epoch=epoch,
                segment=base, gen=id);
            zone = zone + 1;
        }
        replies = 0;
        successes = 0;
        while (replies < sizeof(segmentBuckets[id])) {
            receive { case eCreateResponse: (r: tCreateResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) {
                successes = successes + 1;
            }
        }
        if (successes < 2) { return false; }
        announce eSegmentOpened, (
            segment=base, writerId=writerId, epoch=epoch, gen=id);
        return true;
    }

    fun RegisterPending(id: int): bool {
        var response: tManifestCasResponse;
        var next: tManifestRecord;
        if (view.pending >= 0) { return false; }
        next = (epoch=epoch, owner=writerId,
            tailBase=view.tailBase, tailGen=view.tailGen,
            pending=id,
            sealBase=view.sealBase, sealId=view.sealId,
            sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
            directory=view.directory);
        send manifest, eManifestCas, (
            caller=this, expMetagen=viewMetagen, rec=next);
        receive { case eManifestCasResponse: (r: tManifestCasResponse) {
            response = r;
        } }
        if (response.status != STATUS_OK) { return false; }
        view = next;
        viewMetagen = response.metagen;
        return true;
    }

    fun AppendAndAck(id: int, record: tRecord): bool {
        var zone: int;
        var replies: int;
        var support: int;
        var response: tAppendResponse;
        announce eRecordFormed, (writerId=writerId, record=record);
        zone = 0;
        while (zone < sizeof(segmentBuckets[id])) {
            send segmentBuckets[id][zone], eAppend, (
                caller=this, writerId=writerId, epoch=epoch,
                gen=id, record=record);
            zone = zone + 1;
        }
        replies = 0;
        support = 0;
        while (replies < sizeof(segmentBuckets[id])) {
            receive { case eAppendResponse: (r: tAppendResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) { support = support + 1; }
        }
        if (support < 2) { return false; }
        announce eRecordCommitted, (writerId=writerId, record=record);
        announce eRecordAcknowledged, (
            writerId=writerId, record=record, segmentId=id);
        announce eProducerAck, (
            writerId=writerId, offset=record.offset);
        return true;
    }

    fun AppendWithoutQuorum(id: int, zone: int, record: tRecord): bool {
        var response: tAppendResponse;
        announce eRecordFormed, (writerId=writerId, record=record);
        send segmentBuckets[id][zone], eAppend, (
            caller=this, writerId=writerId, epoch=epoch,
            gen=id, record=record);
        receive { case eAppendResponse: (r: tAppendResponse) {
            response = r;
        } }
        return response.status == STATUS_OK;
    }

    fun FoldTail(refill: int): bool {
        var response: tManifestCasResponse;
        var next: tManifestRecord;
        var nextDirectory: seq[tDirectoryEntry];
        if (view.pending < 0) { return false; }
        nextDirectory = view.directory;
        nextDirectory += (sizeof(nextDirectory), (
            base=view.tailBase, id=view.tailGen));
        next = (epoch=epoch, owner=writerId,
            tailBase=1, tailGen=view.pending, pending=refill,
            sealBase=0, sealId=0, sealEnd=1, sealSum=400,
            trunc=view.trunc, directory=nextDirectory);
        send manifest, eManifestCas, (
            caller=this, expMetagen=viewMetagen, rec=next);
        receive { case eManifestCasResponse: (r: tManifestCasResponse) {
            response = r;
        } }
        if (response.status != STATUS_OK) { return false; }
        view = next;
        viewMetagen = response.metagen;
        announce eViewCommitted, (
            epoch=epoch, tailBase=next.tailBase,
            sealBase=next.sealBase, sealEnd=next.sealEnd,
            sealSum=next.sealSum);
        return true;
    }

    fun Done(committedEnd: int, maintenanceLanded: bool,
        pendingExhausted: bool) {
        send parent, ePendingWriterDone, (
            committedEnd=committedEnd,
            maintenanceLanded=maintenanceLanded,
            pendingExhausted=pendingExhausted);
    }
}

// A compact reusable one-record rotation used by the nondeterministic
// directory lifecycle driver. Each invocation commits one new directory entry
// and then independently may crash, delay maintenance, fail finalization, or
// establish the exact quorum that opens the next rotation gate.
machine DirectoryRotation {
    start state Run {
        entry (payload: (
            writerId: int,
            base: int,
            value: int,
            manifest: ManifestRegister,
            buckets: seq[ZonalBucket],
            parent: machine
        )) {
            var zone: int;
            var replies: int;
            var support: int;
            var finalizeCount: int;
            var lanes: set[int];
            var finalized: set[int];
            var created: tCreateResponse;
            var appended: tAppendResponse;
            var finalizedResponse: tFinalizeResponse;
            var readResponse: tManifestReadResponse;
            var casResponse: tManifestCasResponse;
            var view: tManifestRecord;
            var next: tManifestRecord;
            var nextDirectory: seq[tDirectoryEntry];
            var record: tRecord;
            var sealId: int;

            send payload.manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            if (readResponse.status != STATUS_OK ||
                readResponse.rec.tailBase != payload.base) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            view = readResponse.rec;
            next = (epoch=view.epoch + 1, owner=payload.writerId,
                tailBase=view.tailBase, tailGen=view.tailGen,
                pending=view.pending,
                sealBase=view.sealBase, sealId=view.sealId,
                sealEnd=view.sealEnd, sealSum=view.sealSum, trunc=view.trunc,
                directory=view.directory);
            send payload.manifest, eManifestCas, (
                caller=this, expMetagen=readResponse.metagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            if (casResponse.status != STATUS_OK) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            view = next;
            announce eEpochClaimed, (
                epoch=view.epoch, writerId=payload.writerId);

            zone = 0;
            while (zone < sizeof(payload.buckets)) {
                send payload.buckets[zone], eCreateSegment, (
                    caller=this, writerId=payload.writerId,
                    epoch=view.epoch, segment=payload.base,
                    gen=view.tailGen);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < sizeof(payload.buckets)) {
                receive { case eCreateResponse: (r: tCreateResponse) {
                    created = r;
                } }
                replies = replies + 1;
                if (created.status == STATUS_OK) { lanes += (created.zone); }
            }
            if (sizeof(lanes) < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            announce eSegmentOpened, (
                segment=payload.base, writerId=payload.writerId,
                epoch=view.epoch, gen=view.tailGen);
            record = (offset=payload.base, value=payload.value,
                segment=payload.base);
            announce eRecordFormed, (
                writerId=payload.writerId, record=record);
            foreach (zone in lanes) {
                send payload.buckets[zone], eAppend, (
                    caller=this, writerId=payload.writerId,
                    epoch=view.epoch, gen=view.tailGen, record=record);
            }
            replies = 0;
            support = 0;
            while (replies < sizeof(lanes)) {
                receive { case eAppendResponse: (r: tAppendResponse) {
                    appended = r;
                } }
                replies = replies + 1;
                if (appended.status == STATUS_OK) { support = support + 1; }
            }
            if (support < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=false,
                    sealed=false, crashed=false);
                return;
            }
            announce eRecordCommitted, (
                writerId=payload.writerId, record=record);
            announce eRecordAcknowledged, (
                writerId=payload.writerId, record=record,
                segmentId=view.tailGen);
            announce eProducerAck, (
                writerId=payload.writerId, offset=record.offset);

            sealId = payload.base * 10 + view.tailGen + 1;
            nextDirectory = view.directory;
            nextDirectory += (sizeof(nextDirectory), (
                base=payload.base, id=sealId));
            next = (epoch=view.epoch, owner=payload.writerId,
                tailBase=payload.base + 1, tailGen=view.tailGen,
                pending=view.pending,
                sealBase=payload.base, sealId=sealId,
                sealEnd=payload.base + 1, sealSum=payload.value,
                trunc=view.trunc, directory=nextDirectory);
            send payload.manifest, eManifestCas, (
                caller=this, expMetagen=casResponse.metagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            if (casResponse.status != STATUS_OK) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }
            view = next;
            announce eViewCommitted, (
                epoch=view.epoch, tailBase=view.tailBase,
                sealBase=view.sealBase, sealEnd=view.sealEnd,
                sealSum=view.sealSum);

            if ($) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=true);
                return;
            }
            if ($) {
                // Maintenance has not yet attempted the committed seal.
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }

            finalizeCount = sizeof(payload.buckets);
            if ($) { finalizeCount = 1; }
            zone = 0;
            while (zone < finalizeCount) {
                send payload.buckets[zone], eFinalize, (
                    caller=this, writerId=payload.writerId,
                    epoch=view.epoch, segment=payload.base,
                    gen=view.tailGen, validEnd=payload.base);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < finalizeCount) {
                receive { case eFinalizeResponse: (r: tFinalizeResponse) {
                    finalizedResponse = r;
                } }
                replies = replies + 1;
                if (finalizedResponse.status == STATUS_OK) {
                    finalized += (finalizedResponse.zone);
                }
            }
            if (sizeof(finalized) < 2) {
                send payload.parent, eWriterDone, (
                    writerId=payload.writerId, committed=true,
                    sealed=false, crashed=false);
                return;
            }
            announce eSealQuorumEnforced, (
                segment=payload.base, segmentId=sealId,
                endOffset=payload.base);
            announce eSegmentSealed, (
                segment=payload.base, endOffset=payload.base);
            announce eRotationGateReleased, (
                segment=payload.base, segmentId=sealId,
                endOffset=payload.base);
            send payload.parent, eWriterDone, (
                writerId=payload.writerId, committed=true,
                sealed=true, crashed=false);
        }

        ignore eCreateResponse, eAppendResponse, eFinalizeResponse,
            eManifestReadResponse, eManifestCasResponse;
    }
}

// One tombstone deletion pass. A failed or sleeping zone leaves the directory
// entry in place; a later invocation retries and commits removal only when
// every zone's response proves the exact object absent.
machine DirectoryCleanupCoordinator {
    start state Run {
        entry (payload: (
            buckets: seq[ZonalBucket],
            manifest: ManifestRegister,
            directoryEntry: tDirectoryEntry,
            floor: int,
            raiseFloor: bool,
            parent: machine
        )) {
            var zone: int;
            var index: int;
            var replies: int;
            var found: bool;
            var endOffset: int;
            var absentZones: set[int];
            var readResponse: tManifestReadResponse;
            var casResponse: tManifestCasResponse;
            var deleteResponse: tDeleteResponse;
            var removeResponse: tDirectoryRemoveResponse;
            var current: tManifestRecord;
            var currentMetagen: int;
            var next: tManifestRecord;

            announce eTruncationProposed, payload.floor;
            send payload.manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            if (readResponse.status != STATUS_OK) {
                send payload.parent, eTruncationDone;
                return;
            }
            current = readResponse.rec;
            currentMetagen = readResponse.metagen;
            if (payload.raiseFloor && current.trunc < payload.floor) {
                next = (epoch=current.epoch, owner=current.owner,
                    tailBase=current.tailBase, tailGen=current.tailGen,
                    pending=current.pending,
                    sealBase=current.sealBase, sealId=current.sealId,
                    sealEnd=current.sealEnd, sealSum=current.sealSum,
                    trunc=payload.floor, directory=current.directory);
                send payload.manifest, eManifestCas, (
                    caller=this, expMetagen=currentMetagen, rec=next);
                receive { case eManifestCasResponse: (
                    r: tManifestCasResponse
                ) { casResponse = r; } }
                if (casResponse.status != STATUS_OK) {
                    send payload.parent, eTruncationDone;
                    return;
                }
                current = next;
                currentMetagen = casResponse.metagen;
            }
            announce eFloorCommitted, current.trunc;
            if (current.trunc < payload.floor) {
                send payload.parent, eTruncationDone;
                return;
            }

            found = false;
            index = 0;
            while (index < sizeof(current.directory)) {
                if (current.directory[index] == payload.directoryEntry) {
                    found = true;
                    break;
                }
                index = index + 1;
            }
            if (!found) {
                send payload.parent, eTruncationDone;
                return;
            }
            endOffset = current.tailBase - 1;
            if (index + 1 < sizeof(current.directory)) {
                endOffset = current.directory[index + 1].base - 1;
            }
            if (endOffset >= current.trunc) {
                send payload.parent, eTruncationDone;
                return;
            }
            zone = 0;
            while (zone < sizeof(payload.buckets)) {
                send payload.buckets[zone], eDeleteSegment, (
                    caller=this, segment=payload.directoryEntry.base,
                    endOffset=endOffset, floor=current.trunc);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < sizeof(payload.buckets)) {
                receive { case eDeleteResponse: (r: tDeleteResponse) {
                    deleteResponse = r;
                } }
                replies = replies + 1;
                // The modeled exact delete is idempotent: STATUS_OK means the
                // addressed copy was deleted or was already absent.
                if (deleteResponse.status == STATUS_OK) {
                    absentZones += (deleteResponse.zone);
                }
            }
            if (sizeof(absentZones) == sizeof(payload.buckets)) {
                send payload.manifest, eDirectoryRemove, (
                    caller=this, expMetagen=currentMetagen,
                    directoryEntry=payload.directoryEntry, floor=current.trunc,
                    absentZones=absentZones);
                receive { case eDirectoryRemoveResponse: (
                    r: tDirectoryRemoveResponse
                ) { removeResponse = r; } }
            }
            send payload.parent, eTruncationDone;
        }

        ignore eDeleteResponse, eManifestReadResponse, eManifestCasResponse,
            eDirectoryRemoveResponse;
    }
}

machine SealedRepairCoordinator {
    start state Run {
        entry (payload: (
            bucket: ZonalBucket,
            parent: machine,
            records: seq[tRecord],
            endOffset: int
        )) {
            var response: tRepairResponse;
            send payload.bucket, eRepairSealed, (
                caller=this, segment=0, records=payload.records,
                validEnd=payload.endOffset);
            receive { case eRepairResponse: (r: tRepairResponse) {
                response = r;
            } }
            assert response.status == STATUS_OK,
                "sealed immutable copy should be repairable after rejoin";
            send payload.parent, eRepairDone;
        }
    }
}

// Startup repair for one historical manifest-directory entry. The production
// directory carries a CRC32C; `expectedChecksum` is its equality-only model
// surrogate. Recovery may source repair from one exact checksum match, but
// readiness still requires a restored quorum.
machine HistoricalRecoveryCoordinator {
    start state Run {
        entry (payload: (
            buckets: seq[ZonalBucket],
            directoryEntry: tDirectoryEntry,
            expectedChecksum: int,
            endOffset: int,
            parent: machine
        )) {
            var zone: int;
            var replies: int;
            var response: tReadResponse;
            var repairResponse: tRepairResponse;
            var healthy: set[int];
            var repairBuckets: seq[ZonalBucket];
            var canonical: seq[tRecord];
            var haveCanonical: bool;

            zone = 0;
            while (zone < sizeof(payload.buckets)) {
                send payload.buckets[zone], eRead, (
                    caller=this, segment=payload.directoryEntry.base, gen=-1);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < sizeof(payload.buckets)) {
                receive { case eReadResponse: (r: tReadResponse) {
                    response = r;
                } }
                replies = replies + 1;
                if (response.status == STATUS_OK && response.finalized &&
                    sizeof(response.records) ==
                        payload.endOffset - payload.directoryEntry.base + 1 &&
                    Checksum(response.records) == payload.expectedChecksum) {
                    healthy += (response.zone);
                    canonical = response.records;
                    haveCanonical = true;
                } else if (response.status != STATUS_TRANSIENT) {
                    repairBuckets += (sizeof(repairBuckets),
                        payload.buckets[response.zone]);
                }
            }
            if (haveCanonical) {
                zone = 0;
                while (zone < sizeof(repairBuckets)) {
                    send repairBuckets[zone], eRepairSealed, (
                        caller=this,
                        segment=payload.directoryEntry.base,
                        records=canonical,
                        validEnd=payload.endOffset);
                    zone = zone + 1;
                }
                replies = 0;
                while (replies < sizeof(repairBuckets)) {
                    receive { case eRepairResponse: (r: tRepairResponse) {
                        repairResponse = r;
                    } }
                    replies = replies + 1;
                    if (repairResponse.status == STATUS_OK) {
                        healthy += (repairResponse.zone);
                    }
                }
            }
            announce eHistoricalRecoveryReady, (
                segment=payload.directoryEntry.base,
                checksum=payload.expectedChecksum,
                healthyZones=healthy);
            send payload.parent, eRepairDone;
        }
    }

    fun Checksum(records: seq[tRecord]): int {
        var index: int;
        var checksum: int;
        index = 0;
        checksum = 0;
        while (index < sizeof(records)) {
            checksum = checksum + records[index].value * (index + 1);
            index = index + 1;
        }
        return checksum;
    }
}

machine TruncationCoordinator {
    start state Run {
        entry (payload: (
            buckets: seq[ZonalBucket], manifest: ManifestRegister,
            parent: machine
        )) {
            var zone: int;
            var replies: int;
            var response: tDeleteResponse;
            var readResponse: tManifestReadResponse;
            var casResponse: tManifestCasResponse;
            var removeResponse: tDirectoryRemoveResponse;
            var next: tManifestRecord;
            var current: tManifestRecord;
            var currentMetagen: int;
            var tombstoneEntry: tDirectoryEntry;
            var found: bool;
            var absentZones: set[int];
            announce eTruncationProposed, 2;
            // raise the committed floor before deleting anything
            send payload.manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            current = readResponse.rec;
            currentMetagen = readResponse.metagen;
            if (readResponse.rec.trunc < 2) {
                next = (epoch=readResponse.rec.epoch,
                    owner=readResponse.rec.owner,
                    tailBase=readResponse.rec.tailBase,
                    tailGen=readResponse.rec.tailGen,
                    pending=readResponse.rec.pending,
                    sealBase=readResponse.rec.sealBase,
                    sealId=readResponse.rec.sealId,
                    sealEnd=readResponse.rec.sealEnd,
                    sealSum=readResponse.rec.sealSum, trunc=2,
                    directory=readResponse.rec.directory);
                send payload.manifest, eManifestCas, (
                    caller=this, expMetagen=readResponse.metagen, rec=next);
                receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                    casResponse = r;
                } }
                if (casResponse.status != STATUS_OK) {
                    send payload.parent, eTruncationDone;
                    return;
                }
                current = next;
                currentMetagen = casResponse.metagen;
            }
            announce eFloorCommitted, 2;
            zone = 0;
            while (zone < sizeof(payload.buckets)) {
                send payload.buckets[zone], eDeleteSegment, (
                    caller=this, segment=0, endOffset=1, floor=2);
                zone = zone + 1;
            }
            replies = 0;
            while (replies < sizeof(payload.buckets)) {
                receive { case eDeleteResponse: (r: tDeleteResponse) {
                    response = r;
                } }
                replies = replies + 1;
                if (response.status == STATUS_OK) {
                    absentZones += (response.zone);
                }
            }
            found = false;
            zone = 0;
            while (zone < sizeof(current.directory)) {
                if (current.directory[zone].base == 0) {
                    tombstoneEntry = current.directory[zone];
                    found = true;
                }
                zone = zone + 1;
            }
            if (found && sizeof(absentZones) == sizeof(payload.buckets)) {
                send payload.manifest, eDirectoryRemove, (
                    caller=this, expMetagen=currentMetagen,
                    directoryEntry=tombstoneEntry,
                    floor=2, absentZones=absentZones);
                receive { case eDirectoryRemoveResponse: (
                    r: tDirectoryRemoveResponse
                ) { removeResponse = r; } }
            }
            send payload.parent, eTruncationDone;
        }

        ignore eDeleteResponse, eManifestReadResponse, eManifestCasResponse,
            eDirectoryRemoveResponse;
    }
}

machine StartupReplay {
    start state Run {
        entry (payload: (reader: int, parent: machine)) {
            announce eReplayOpened, (
                reader=payload.reader, startOffset=0, endOffset=2);
            announce eReplayRecord, (reader=payload.reader, offset=0);
            announce eReplayRecord, (reader=payload.reader, offset=1);
            announce eReplayClosed, payload.reader;
            send payload.parent, eReplayDone;
        }
    }
}

machine GetSizeProbe {
    start state Run {
        entry (payload: (bucket: ZonalBucket, parent: machine)) {
            var response: tGetResponse;
            send payload.bucket, eGetObject, (caller=this,);
            receive { case eGetResponse: (r: tGetResponse) {
                response = r;
            } }
            assert response.status == STATUS_OK &&
                (response.finalized || response.reportedSize == 0),
                "GetObject.size exposed an unfinalized append tail";
            send payload.parent, eReplayDone;
        }
    }
}
