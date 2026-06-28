// Persistent readonly follower over the manifest-selected segment chain.
//
// The follower never claims an epoch and never sends a mutating storage
// request. Each poll reads one linearizable manifest snapshot, detects an
// overtaking truncation floor, reads finalized directory segments, then reads
// the active tail without requiring finalization. Active records are emitted
// only after identical bytes are visible on two zones. The active read returns
// as soon as the first matching majority arrives; a slow third response is not
// on the publication path. Polling the same machine again discovers later
// appends and seals without writer coordination.
machine ReadonlyFollower {
    var reader: int;
    var manifest: ManifestRegister;
    var segmentBuckets: seq[seq[ZonalBucket]];
    var parent: machine;
    var nextOffset: int;
    var pauseAfterSnapshot: bool;
    var paused: bool;

    start state Following {
        entry (payload: (
            reader: int,
            manifest: ManifestRegister,
            segmentBuckets: seq[seq[ZonalBucket]],
            nextOffset: int,
            pauseAfterSnapshot: bool,
            parent: machine
        )) {
            reader = payload.reader;
            manifest = payload.manifest;
            segmentBuckets = payload.segmentBuckets;
            nextOffset = payload.nextOffset;
            pauseAfterSnapshot = payload.pauseAfterSnapshot;
            parent = payload.parent;
            announce eReadonlyOpened, (
                reader=reader, nextOffset=nextOffset);
        }

        on eReadonlyPoll do Poll;

        ignore eManifestReadResponse, eReadResponse, eReadonlyContinue;
    }

    fun Poll() {
        var readResponse: tManifestReadResponse;
        var emitted: int;
        var index: int;
        var endOffset: int;
        var found: bool;

        send manifest, eManifestRead, (caller=this,);
        receive { case eManifestReadResponse: (r: tManifestReadResponse) {
            readResponse = r;
        } }
        if (readResponse.status != STATUS_OK) {
            Done(0, false);
            return;
        }
        if (readResponse.rec.trunc > nextOffset) {
            announce eReadonlyLagged, (
                reader=reader, nextOffset=nextOffset,
                trunc=readResponse.rec.trunc);
            Done(0, true);
            return;
        }

        emitted = 0;
        while (nextOffset < readResponse.rec.tailBase) {
            found = false;
            index = 0;
            while (index < sizeof(readResponse.rec.directory)) {
                endOffset = DirectoryEnd(readResponse.rec, index);
                if (readResponse.rec.directory[index].base <= nextOffset &&
                    nextOffset <= endOffset) {
                    found = true;
                    break;
                }
                index = index + 1;
            }
            assert found,
                "manifest published end without a directory segment";

            announce eReadonlySnapshot, (
                reader=reader,
                nextOffset=nextOffset,
                trunc=readResponse.rec.trunc,
                publishedEnd=readResponse.rec.tailBase,
                segmentBase=readResponse.rec.directory[index].base,
                segmentId=readResponse.rec.directory[index].id,
                segmentEnd=endOffset);
            if (pauseAfterSnapshot && !paused) {
                paused = true;
                send parent, eReadonlySnapshotPaused;
                receive { case eReadonlyContinue: {} }
            }

            if (!ReadOne(
                readResponse.rec.directory[index],
                endOffset
            )) {
                Done(emitted, false);
                return;
            }
            nextOffset = nextOffset + 1;
            emitted = emitted + 1;
        }
        if (readResponse.rec.tailBase < sizeof(segmentBuckets)) {
            announce eReadonlyActiveSnapshot, (
                reader=reader,
                nextOffset=nextOffset,
                trunc=readResponse.rec.trunc,
                segmentBase=readResponse.rec.tailBase,
                segmentId=readResponse.rec.tailGen);
            if (ReadActive(
                readResponse.rec.tailBase,
                readResponse.rec.tailGen
            )) {
                nextOffset = nextOffset + 1;
                emitted = emitted + 1;
            }
        }
        Done(emitted, false);
    }

    fun ReadOne(directoryEntry: tDirectoryEntry, endOffset: int): bool {
        var zone: int;
        var replies: int;
        var response: tReadResponse;
        var record: tRecord;
        var candidate: tRecord;
        var foundRecord: bool;
        var key: (offset: int, value: int, segment: int);
        var support: map[
            (offset: int, value: int, segment: int), set[int]
        ];

        zone = 0;
        while (zone < sizeof(segmentBuckets[directoryEntry.base])) {
            send segmentBuckets[directoryEntry.base][zone], eRead, (
                caller=this, segment=directoryEntry.base, gen=-1);
            zone = zone + 1;
        }
        replies = 0;
        foundRecord = false;
        while (replies < sizeof(segmentBuckets[directoryEntry.base])) {
            receive { case eReadResponse: (r: tReadResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK && response.finalized) {
                foreach (record in response.records) {
                    if (record.offset == nextOffset &&
                        record.offset <= endOffset) {
                        key = (
                            offset=record.offset,
                            value=record.value,
                            segment=record.segment);
                        if (!(key in support)) {
                            support[key] = default(set[int]);
                        }
                        support[key] += (response.zone);
                        if (sizeof(support[key]) >= 2) {
                            candidate = record;
                            foundRecord = true;
                        }
                    }
                }
            }
        }
        if (!foundRecord) { return false; }
        announce eReadonlyRecord, (
            reader=reader, record=candidate, segmentId=directoryEntry.id);
        return true;
    }

    fun ReadActive(segmentBase: int, segmentId: int): bool {
        var zone: int;
        var replies: int;
        var response: tReadResponse;
        var record: tRecord;
        var key: (offset: int, value: int, segment: int);
        var support: map[
            (offset: int, value: int, segment: int), set[int]
        ];

        zone = 0;
        while (zone < sizeof(segmentBuckets[segmentBase])) {
            send segmentBuckets[segmentBase][zone], eRead, (
                caller=this, segment=segmentBase, gen=segmentId);
            zone = zone + 1;
        }
        replies = 0;
        while (replies < sizeof(segmentBuckets[segmentBase])) {
            receive { case eReadResponse: (r: tReadResponse) {
                response = r;
            } }
            replies = replies + 1;
            if (response.status == STATUS_OK) {
                foreach (record in response.records) {
                    if (record.offset == nextOffset) {
                        key = (
                            offset=record.offset,
                            value=record.value,
                            segment=record.segment);
                        if (!(key in support)) {
                            support[key] = default(set[int]);
                        }
                        support[key] += (response.zone);
                        if (sizeof(support[key]) >= 2) {
                            announce eReadonlyRecord, (
                                reader=reader, record=record,
                                segmentId=segmentId);
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    fun DirectoryEnd(record: tManifestRecord, index: int): int {
        if (index + 1 < sizeof(record.directory)) {
            return record.directory[index + 1].base - 1;
        }
        return record.tailBase - 1;
    }

    fun Done(emitted: int, lagged: bool) {
        send parent, eReadonlyPollDone, (
            reader=reader, nextOffset=nextOffset,
            emitted=emitted, lagged=lagged);
    }
}

machine ReadonlyFollowerDriver {
    start state Init {
        entry {
            var segmentBuckets: seq[seq[ZonalBucket]];
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var follower: ReadonlyFollower;
            var base: int;
            var zone: int;
            var writerId: int;
            var stopped: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            );
            var poll: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            );

            base = 0;
            while (base < 2) {
                buckets = default(seq[ZonalBucket]);
                zone = 0;
                while (zone < 3) {
                    buckets += (zone,
                        new ZonalBucket((zone=zone, failures=0)));
                    zone = zone + 1;
                }
                segmentBuckets += (base, buckets);
                base = base + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            follower = new ReadonlyFollower((
                reader=100, manifest=manifest,
                segmentBuckets=segmentBuckets, nextOffset=0,
                pauseAfterSnapshot=false, parent=this));

            // Opening and polling before the first seal observes an empty
            // published prefix without creating or claiming anything.
            send follower, eReadonlyPoll;
            receive { case eReadonlyPollDone: (payload: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            )) { poll = payload; } }
            assert poll.nextOffset == 0 && poll.emitted == 0 &&
                !poll.lagged;

            writerId = 1;
            base = 0;
            while (base < 2) {
                new DirectoryRotation((
                    writerId=writerId, base=base,
                    value=700 + base, manifest=manifest,
                    buckets=segmentBuckets[base], parent=this));
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool,
                    sealed: bool, crashed: bool
                )) { stopped = payload; } }
                assert stopped.committed;
                writerId = writerId + 1;
                if (!stopped.sealed) {
                    new WriterProcess((
                        writerId=writerId, value=-1,
                        buckets=segmentBuckets[base], segBase=base,
                        manifest=manifest, parent=this,
                        shouldSeal=true, recoverExisting=true,
                        crashBudget=0));
                    receive { case eWriterDone: (payload: (
                        writerId: int, committed: bool,
                        sealed: bool, crashed: bool
                    )) { stopped = payload; } }
                    assert stopped.sealed;
                    writerId = writerId + 1;
                }

                send follower, eReadonlyPoll;
                receive { case eReadonlyPollDone: (payload: (
                    reader: int, nextOffset: int,
                    emitted: int, lagged: bool
                )) { poll = payload; } }
                assert poll.nextOffset == base + 1 &&
                    poll.emitted == 1 && !poll.lagged,
                    "readonly follower did not discover the next seal";
                base = base + 1;
            }
        }
    }
}

machine ReadonlyActiveTailDriver {
    start state Init {
        entry {
            var segmentBuckets: seq[seq[ZonalBucket]];
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var follower: ReadonlyFollower;
            var zone: int;
            var stopped: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            );
            var poll: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            );

            zone = 0;
            while (zone < 3) {
                buckets += (zone,
                    new ZonalBucket((zone=zone, failures=0)));
                zone = zone + 1;
            }
            segmentBuckets += (0, buckets);
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((
                writerId=1, value=750,
                buckets=buckets, segBase=0,
                manifest=manifest, parent=this,
                shouldSeal=false, recoverExisting=false,
                crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed && !stopped.sealed;

            follower = new ReadonlyFollower((
                reader=102, manifest=manifest,
                segmentBuckets=segmentBuckets, nextOffset=0,
                pauseAfterSnapshot=false, parent=this));
            send follower, eReadonlyPoll;
            receive { case eReadonlyPollDone: (payload: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            )) { poll = payload; } }
            assert poll.nextOffset == 1 && poll.emitted == 1 &&
                !poll.lagged,
                "readonly follower did not read the quorum-visible active tail";

            // A later recovery must retain and seal the majority-visible frame.
            new WriterProcess((
                writerId=2, value=-1,
                buckets=buckets, segBase=0,
                manifest=manifest, parent=this,
                shouldSeal=true, recoverExisting=true,
                crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
        }
    }
}

machine ReadonlyTruncationRaceDriver {
    start state Init {
        entry {
            var segmentBuckets: seq[seq[ZonalBucket]];
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var follower: ReadonlyFollower;
            var zone: int;
            var stopped: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            );
            var poll: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            );

            zone = 0;
            while (zone < 3) {
                buckets += (zone,
                    new ZonalBucket((zone=zone, failures=0)));
                zone = zone + 1;
            }
            segmentBuckets += (0, buckets);
            manifest = new ManifestRegister((failures=0,));

            new DirectoryRotation((
                writerId=1, base=0, value=800,
                manifest=manifest, buckets=buckets, parent=this));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;

            follower = new ReadonlyFollower((
                reader=101, manifest=manifest,
                segmentBuckets=segmentBuckets, nextOffset=0,
                pauseAfterSnapshot=true, parent=this));
            send follower, eReadonlyPoll;
            receive { case eReadonlySnapshotPaused: {} }

            // The follower holds a valid old snapshot while truncation raises
            // the floor and deletes every copy. Its stale object read fails;
            // the next poll must refresh and report lag, never skip to one.
            new DirectoryCleanupCoordinator((
                buckets=buckets, manifest=manifest,
                directoryEntry=(base=0, id=1),
                floor=1, raiseFloor=true, parent=this));
            receive { case eTruncationDone: {} }
            send follower, eReadonlyContinue;
            receive { case eReadonlyPollDone: (payload: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            )) { poll = payload; } }
            assert poll.nextOffset == 0 && poll.emitted == 0 &&
                !poll.lagged;

            send follower, eReadonlyPoll;
            receive { case eReadonlyPollDone: (payload: (
                reader: int, nextOffset: int,
                emitted: int, lagged: bool
            )) { poll = payload; } }
            assert poll.nextOffset == 0 && poll.emitted == 0 &&
                poll.lagged,
                "readonly follower skipped an overtaking truncation floor";
        }
    }
}
