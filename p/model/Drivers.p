// Scenario drivers. Each builds three zonal buckets plus the regional
// manifest register, then exercises one adversarial slice. Writer crash
// points and bucket/manifest fault budgets give the checker nondeterministic
// interleavings within each scenario.

machine ConcurrentWritersDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var done: int;
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=1)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=1,));
            new WriterProcess((writerId=1, value=10, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            new WriterProcess((writerId=2, value=20, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            done = 0;
            while (done < 2) {
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool, sealed: bool, crashed: bool
                )) { done = done + 1; } }
            }
        }
    }
}

machine TwoZoneRecoveryDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            send buckets[2], eCrash;
            new WriterProcess((writerId=1, value=11, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;

            // Recovery can see the committed tail on only zone 1. Zone 2 is a
            // reachable empty witness; write-back promotes the tail to both.
            send buckets[0], eCrash;
            send buckets[2], eRestart;
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
        }
    }
}

machine FinalizedGenerationRecoveryDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((writerId=1, value=31, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed && stopped.sealed;

            // Production can conditionally replace a finalized generation.
            // Recovery rewrites the exact prefix into a new appendable
            // generation, then finalizes and re-enforces the committed seal.
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
        }
    }
}

machine ProgressDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((writerId=1, value=42, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;
        }
    }
}

machine PipelinedRecordTruncationDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var none: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new PipelinedWriter((writerId=1, manifest=manifest,
                buckets=buckets, nextBuckets=none, parent=this));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed && stopped.sealed;

            new StartupReplay((reader=1, parent=this));
            receive { case eReplayDone: {} }
            new TruncationCoordinator((buckets=buckets, manifest=manifest,
                parent=this));
            receive { case eTruncationDone: {} }
        }
    }
}

machine SealedRepairDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var records: seq[tRecord];
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            send buckets[2], eCrash;
            new WriterProcess((writerId=1, value=77, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed && stopped.sealed;
            send buckets[2], eRestart;
            records += (0, (offset=0, value=77, segment=0));
            new SealedRepairCoordinator((bucket=buckets[2], parent=this,
                records=records, endOffset=0));
            receive { case eRepairDone: {} }
        }
    }
}

// An older sealed segment has one manifest-checksum-valid copy, one corrupt
// reachable copy, and one unavailable copy. Startup must restore the corrupt
// live zone before it declares the historical directory safe.
machine HistoricalRecoveryRepairDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            );
            var readResponse: tManifestReadResponse;
            var casResponse: tManifestCasResponse;
            var objectResponse: tGetResponse;
            var dataResponse: tReadResponse;
            var next: tManifestRecord;
            var nextDirectory: seq[tDirectoryEntry];
            var historical: tDirectoryEntry;

            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((
                writerId=1, value=77, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed && stopped.sealed;

            // Commit a later seal so directory[0] is historical rather than
            // the current seal whose enforcement already runs synchronously.
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            historical = readResponse.rec.directory[0];
            nextDirectory = readResponse.rec.directory;
            nextDirectory += (sizeof(nextDirectory), (base=1, id=11));
            next = (
                epoch=readResponse.rec.epoch,
                owner=readResponse.rec.owner,
                tailBase=2,
                tailGen=readResponse.rec.tailGen,
                pending=readResponse.rec.pending,
                sealBase=1,
                sealId=11,
                sealEnd=2,
                sealSum=88,
                trunc=readResponse.rec.trunc,
                directory=nextDirectory);
            send manifest, eManifestCas, (
                caller=this, expMetagen=readResponse.metagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            assert casResponse.status == STATUS_OK &&
                historical.id != next.sealId;

            // Synchronizing probes ensure the corruption and crash have been
            // applied before the startup repair coordinator reads the zones.
            send buckets[1], eCorruptRecord, (offset=0,);
            send buckets[1], eRead, (
                caller=this, segment=historical.base, gen=-1);
            receive { case eReadResponse: (r: tReadResponse) {
                dataResponse = r;
            } }
            assert dataResponse.status == STATUS_OK &&
                sizeof(dataResponse.records) == 0;
            send buckets[0], eCrash;
            send buckets[0], eGetObject, (caller=this,);
            receive { case eGetResponse: (r: tGetResponse) {
                objectResponse = r;
            } }
            assert objectResponse.status == STATUS_TRANSIENT;

            new HistoricalRecoveryCoordinator((
                buckets=buckets,
                directoryEntry=historical,
                expectedChecksum=77,
                endOffset=0,
                parent=this));
            receive { case eRepairDone: {} }
        }
    }
}

machine GetSizeDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            buckets += (0, new ZonalBucket((zone=0, failures=0)));
            buckets += (1, new ZonalBucket((zone=1, failures=0)));
            buckets += (2, new ZonalBucket((zone=2, failures=0)));
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((writerId=1, value=55, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;
            new GetSizeProbe((bucket=buckets[0], parent=this));
            receive { case eReplayDone: {} }
        }
    }
}

// Two recoverers race over divergent witnesses while one zone stays down
// for the whole scenario. The register serializes their claims and seal
// commits; ManifestSafety rejects any second decision for the segment.
machine RacingRecoveriesDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var done: int;
            var sealedCount: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=1)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=1,));
            send buckets[2], eCrash;
            new WriterProcess((writerId=1, value=61, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=1));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }

            send buckets[0], eCrash;
            send buckets[2], eRestart;
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=1));
            new WriterProcess((writerId=3, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=1));
            done = 0;
            while (done < 2) {
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool, sealed: bool, crashed: bool
                )) { done = done + 1; } }
            }
        }
    }
}

// Pure ambiguity, no scripted outages: every zonal op and the manifest CAS
// may apply and lose its response, and every process may crash. The durable
// tail can be shorter or longer than any client believes; racing recoverers
// must still converge on at most one seal decision, and one-witness
// promotion must never erase an acknowledged record.
machine AmbiguousTailPromotionDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var done: int;
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=1)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=1,));
            new WriterProcess((writerId=1, value=91, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=1));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { done = 1; } }

            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=1));
            new WriterProcess((writerId=3, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=1));
            done = 0;
            while (done < 2) {
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool, sealed: bool, crashed: bool
                )) { done = done + 1; } }
            }
        }
    }
}

// CRC rot on one lane at a time. Recovery must select the canonical prefix
// from the two healthy copies, write it back (clearing the rot), and seal;
// a second rot on an already sealed copy must be repaired by the next
// recovery against the committed (adopted) seal decision.
machine CorruptLaneRecoveryDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new WriterProcess((writerId=1, value=71, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;

            // rot the committed record on one lane; the two healthy copies
            // form the compatible pair and recovery seals through them
            send buckets[0], eCorruptRecord, (offset=0,);
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;

            // rot a different lane of the now sealed segment; recovery must
            // enforce the committed seal decision (adopt, never re-decide)
            // and leave the canonical bytes on a quorum again
            send buckets[1], eCorruptRecord, (offset=0,);
            new WriterProcess((writerId=3, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
        }
    }
}

// The segment chain under deferred-swap rotation: a pipelined writer brings
// segment 0 to its committed boundary, provisions the successor spare and
// admits into it BEFORE the rotation CAS (acknowledgment gated on the CAS),
// then either crashes around the decision, stalls first-seal enforcement, or
// reaches the finalized quorum that opens the gate for a SECOND rotation.
// Any blocked path is recovered by enforcing segment 0's committed decision;
// the successful path commits and finalizes seals for both segments 0 and 2.
machine RotationChainDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var nextBuckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            var readResponse: tManifestReadResponse;
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                nextBuckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));
            new PipelinedWriter((writerId=1, manifest=manifest,
                buckets=buckets, nextBuckets=nextBuckets, parent=this));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;

            if (!stopped.sealed) {
                // This covers the pre-CAS crash, post-CAS/pre-finalize crash,
                // and stalled first-seal enforcement. Recovery either decides
                // the old tail's seal or adopts the committed decision, then
                // establishes the exact quorum required to reopen the gate.
                new WriterProcess((writerId=2, value=-1, buckets=buckets,
                    segBase=0, manifest=manifest, parent=this,
                    shouldSeal=true, recoverExisting=true, crashBudget=0));
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool, sealed: bool, crashed: bool
                )) { stopped = payload; } }
                assert stopped.sealed;
            } else {
                // The uninterrupted path itself performed two rotations.
                // RotationGateSafety observes the second decision only after
                // the first eRotationGateReleased cleared the pending gate.
                assert stopped.committed && stopped.sealed;
                send manifest, eManifestRead, (caller=this,);
                receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                    readResponse = r;
                } }
                assert readResponse.status == STATUS_OK &&
                    readResponse.rec.tailBase == 3 &&
                    readResponse.rec.sealBase == 2 &&
                    readResponse.rec.sealEnd == 3,
                    "uninterrupted pipelined path did not perform two rotations";
            }
        }
    }
}

// Pending-segment rotation verification. The checker chooses among:
// - crash before the off-path maintenance fold;
// - crash after the fold/refill CAS;
// - a recovery claim racing the CAS-free flip into pending; and
// - a second rotation after consuming the spare, which must fail closed; and
// - pre-fold recovery on the two reachable zones after the third goes down.
// Every path finishes with a fenced [tail, pending?] recovery walk whose
// recovered boundary must equal the acknowledged prefix.
machine PendingSegmentRotationDriver {
    start state Init {
        entry {
            var segmentBuckets: seq[seq[ZonalBucket]];
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var writer: PendingSegmentWriter;
            var base: int;
            var zone: int;
            var doneCount: int;
            var pauseBeforeRotation: bool;
            var landMaintenance: bool;
            var attemptSecondRotation: bool;
            var attemptGapRotation: bool;
            var recoverWithOneZoneDown: bool;
            var writerDone: (
                committedEnd: int,
                maintenanceLanded: bool,
                pendingExhausted: bool
            );
            var recoveryDone: (
                writerId: int,
                directoryEnd: int,
                recoveredEnd: int,
                pendingExhausted: bool
            );

            base = 0;
            while (base < 4) {
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

            landMaintenance = false;
            pauseBeforeRotation = false;
            attemptSecondRotation = false;
            attemptGapRotation = false;
            recoverWithOneZoneDown = false;
            if ($) {
                // Post-fold crash: maintenance provisions a refill and lands
                // the combined fold/refill CAS before recovery.
                landMaintenance = true;
            } else if ($) {
                // Recovery claims a higher epoch after preregistration but
                // before the stale writer flips into pending.
                pauseBeforeRotation = true;
            } else if ($) {
                // The first CAS-free flip consumed the only in-memory spare.
                // A second rotation must stop before an unregistered append.
                attemptSecondRotation = true;
            } else if ($) {
                // One old-tail replica stores the next record, but no quorum
                // commits it. Rotation must not splice pending above this gap.
                attemptGapRotation = true;
            } else if ($) {
                // The writer and recovery both retain zones 0 and 1 as a
                // quorum while zone 2 is unavailable.
                recoverWithOneZoneDown = true;
            }

            if (recoverWithOneZoneDown) {
                base = 0;
                while (base < sizeof(segmentBuckets)) {
                    send segmentBuckets[base][2], eCrash;
                    base = base + 1;
                }
            }

            writer = new PendingSegmentWriter((
                writerId=1, manifest=manifest,
                segmentBuckets=segmentBuckets,
                pauseBeforeRotation=pauseBeforeRotation,
                landMaintenance=landMaintenance,
                attemptSecondRotation=attemptSecondRotation,
                attemptGapRotation=attemptGapRotation, parent=this));

            if (pauseBeforeRotation) {
                receive { case ePendingWriterReady: {} }
                new PendingChainRecovery((
                    writerId=2, manifest=manifest,
                    segmentBuckets=segmentBuckets, parent=this));
                send writer, ePendingWriterContinue;
                doneCount = 0;
                while (doneCount < 2) {
                    receive {
                        case ePendingWriterDone: (payload: (
                            committedEnd: int,
                            maintenanceLanded: bool,
                            pendingExhausted: bool
                        )) {
                            writerDone = payload;
                            doneCount = doneCount + 1;
                        }
                        case ePendingRecoveryCompleted: (payload: (
                            writerId: int,
                            directoryEnd: int,
                            recoveredEnd: int,
                            pendingExhausted: bool
                        )) {
                            recoveryDone = payload;
                            doneCount = doneCount + 1;
                        }
                    }
                }
            } else {
                receive { case ePendingWriterDone: (payload: (
                    committedEnd: int,
                    maintenanceLanded: bool,
                    pendingExhausted: bool
                )) { writerDone = payload; } }
                new PendingChainRecovery((
                    writerId=2, manifest=manifest,
                    segmentBuckets=segmentBuckets, parent=this));
                receive { case ePendingRecoveryCompleted: (payload: (
                    writerId: int,
                    directoryEnd: int,
                    recoveredEnd: int,
                    pendingExhausted: bool
                )) { recoveryDone = payload; } }
            }

            assert writerDone.committedEnd == recoveryDone.recoveredEnd,
                "pending recovery boundary differed from acknowledged prefix";
            if (attemptSecondRotation) {
                assert writerDone.committedEnd == 2 &&
                    writerDone.pendingExhausted,
                    "second rotation admitted an unregistered successor";
            }
            if (attemptGapRotation) {
                assert writerDone.committedEnd == 1 &&
                    recoveryDone.recoveredEnd == 1 &&
                    !recoveryDone.pendingExhausted,
                    "rotation crossed an uncommitted old-tail gap";
            }
            if (recoverWithOneZoneDown) {
                assert writerDone.committedEnd == 2 &&
                    recoveryDone.recoveredEnd == 2 &&
                    recoveryDone.pendingExhausted,
                    "pre-fold recovery required all three zones";
            }
            if (!pauseBeforeRotation && landMaintenance) {
                assert writerDone.maintenanceLanded &&
                    recoveryDone.directoryEnd == 1 &&
                    !recoveryDone.pendingExhausted,
                    "post-fold recovery did not adopt the predecessor and refill";
            }
            if (!pauseBeforeRotation && !landMaintenance &&
                !attemptGapRotation) {
                assert !writerDone.maintenanceLanded &&
                    recoveryDone.directoryEnd == 0 &&
                    recoveryDone.pendingExhausted,
                    "pre-fold recovery skipped the manifest tail";
            }
        }
    }
}

// Bounded nondeterministic directory lifecycle: perform two or three
// rotations, independently choosing crash, maintenance delay, finalize
// failure, successful enforcement, and recovery at each gate. Then optionally
// truncate one first/interior/last entry. One zone sleeps through the first
// cleanup pass, proving the entry remains a tombstone until a later retry has
// all-zone absence evidence.
machine DirectoryLifecycleDriver {
    start state Init {
        entry {
            var segmentBuckets: seq[seq[ZonalBucket]];
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var base: int;
            var zone: int;
            var rotations: int;
            var target: int;
            var floor: int;
            var writerId: int;
            var index: int;
            var found: bool;
            var stopped: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            );
            var readResponse: tManifestReadResponse;
            var tombstoneEntry: tDirectoryEntry;

            base = 0;
            while (base < 3) {
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
            rotations = 2;
            if ($) { rotations = 3; }
            writerId = 1;
            base = 0;
            while (base < rotations) {
                new DirectoryRotation((
                    writerId=writerId, base=base, value=100 + base,
                    manifest=manifest, buckets=segmentBuckets[base],
                    parent=this));
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool,
                    sealed: bool, crashed: bool
                )) { stopped = payload; } }
                assert stopped.committed;
                writerId = writerId + 1;

                if (!stopped.sealed) {
                    // The next swap is forbidden until recovery enforces the
                    // current seal.
                    new WriterProcess((
                        writerId=writerId, value=-1,
                        buckets=segmentBuckets[base], segBase=base,
                        manifest=manifest, parent=this, shouldSeal=true,
                        recoverExisting=true, crashBudget=0));
                    receive { case eWriterDone: (payload: (
                        writerId: int, committed: bool,
                        sealed: bool, crashed: bool
                    )) { stopped = payload; } }
                    assert stopped.sealed;
                    writerId = writerId + 1;
                } else if ($) {
                    // Recovery is also legal after successful maintenance and
                    // must adopt older directory entries without rereading.
                    new WriterProcess((
                        writerId=writerId, value=-1,
                        buckets=segmentBuckets[base], segBase=base,
                        manifest=manifest, parent=this, shouldSeal=true,
                        recoverExisting=true, crashBudget=0));
                    receive { case eWriterDone: (payload: (
                        writerId: int, committed: bool,
                        sealed: bool, crashed: bool
                    )) { stopped = payload; } }
                    assert stopped.sealed;
                    writerId = writerId + 1;
                }
                base = base + 1;
            }

            // Force one final prepare_recovery-shaped pass over a directory
            // with an older live seal, then choose whether truncation runs.
            base = rotations - 1;
            new WriterProcess((
                writerId=writerId, value=-1,
                buckets=segmentBuckets[base], segBase=base,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
            writerId = writerId + 1;
            if ($) { return; }

            target = 0;
            if (rotations == 2) {
                if ($) { target = 1; }
            } else {
                if ($) {
                    target = 1;
                } else if ($) {
                    target = 2;
                }
            }
            floor = target + 1;
            tombstoneEntry = (base=target, id=target * 10 + 1);

            // The first pass raises the floor and deletes two copies, but the
            // sleeping zone prevents all-zone absence and therefore removal.
            send segmentBuckets[target][2], eCrash;
            new DirectoryCleanupCoordinator((
                buckets=segmentBuckets[target], manifest=manifest,
                directoryEntry=tombstoneEntry, floor=floor, raiseFloor=true,
                parent=this));
            receive { case eTruncationDone: {} }
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            found = false;
            index = 0;
            while (index < sizeof(readResponse.rec.directory)) {
                if (readResponse.rec.directory[index] == tombstoneEntry) {
                    found = true;
                }
                index = index + 1;
            }
            assert found,
                "tombstone disappeared before every zone proved absence";

            // Recovery observes tombstones but leaves them out of replay. If
            // the current seal itself is wholly truncated, it is not enforced.
            base = rotations - 1;
            new WriterProcess((
                writerId=writerId, value=-1,
                buckets=segmentBuckets[base], segBase=base,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool,
                sealed: bool, crashed: bool
            )) { stopped = payload; } }

            send segmentBuckets[target][2], eRestart;
            new DirectoryCleanupCoordinator((
                buckets=segmentBuckets[target], manifest=manifest,
                directoryEntry=tombstoneEntry, floor=floor, raiseFloor=false,
                parent=this));
            receive { case eTruncationDone: {} }
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            found = false;
            index = 0;
            while (index < sizeof(readResponse.rec.directory)) {
                if (readResponse.rec.directory[index] == tombstoneEntry) {
                    found = true;
                }
                index = index + 1;
            }
            assert !found,
                "tombstone remained after all-zone absence was witnessed";
        }
    }
}

// Retired-name safety: a crashed incarnation leaves unacknowledged bytes on
// exactly one zone, that zone goes dark, and recovery finds the tail empty on
// the surviving quorum. Recovery commits a fresh tail generation through the
// register before declaring the tail empty. The returning zone's stale object
// then answers reads of the current generation NOT_FOUND, recovery creates the
// fresh generation over it, and the log seals at the acknowledged record.
machine StaleReplicaRecoveryDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var stopped: (writerId: int, committed: bool, sealed: bool, crashed: bool);
            var readResponse: tManifestReadResponse;
            var casResponse: tManifestCasResponse;
            var createResponse: tCreateResponse;
            var appendResponse: tAppendResponse;
            var next: tManifestRecord;
            var record: tRecord;
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=0)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=0,));

            // The driver plays the doomed incarnation inline (writer 1,
            // epoch 1, generation 0): claim, create the tail everywhere,
            // persist one record on zone 0 only, never acknowledge it.
            send manifest, eManifestRead, (caller=this,);
            receive { case eManifestReadResponse: (r: tManifestReadResponse) {
                readResponse = r;
            } }
            next = (epoch=1, owner=1, tailBase=0, tailGen=0,
                pending=-1, sealBase=-1,
                sealId=-1, sealEnd=0, sealSum=0, trunc=0,
                directory=default(seq[tDirectoryEntry]));
            send manifest, eManifestCas, (
                caller=this, expMetagen=readResponse.metagen, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) {
                casResponse = r;
            } }
            assert casResponse.status == STATUS_OK;
            announce eEpochClaimed, (epoch=1, writerId=1);
            i = 0;
            while (i < 3) {
                send buckets[i], eCreateSegment, (
                    caller=this, writerId=1, epoch=1, segment=0, gen=0);
                i = i + 1;
            }
            i = 0;
            while (i < 3) {
                receive { case eCreateResponse: (r: tCreateResponse) {
                    createResponse = r;
                } }
                i = i + 1;
            }
            record = (offset=0, value=13, segment=0);
            send buckets[0], eAppend, (
                caller=this, writerId=1, epoch=1, gen=0, record=record);
            receive { case eAppendResponse: (r: tAppendResponse) {
                appendResponse = r;
            } }
            assert appendResponse.status == STATUS_OK;
            // the incarnation dies and its only up-to-date zone goes dark
            send buckets[0], eCrash;

            // Recovery sees two present-but-empty witnesses: the canonical
            // tail is empty. It must retire generation 0 through the
            // register and discard the fenced witnesses.
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert !stopped.crashed;

            // A fresh writer bootstraps generation 1 on the surviving zones
            // and acknowledges a record while zone 0 is still down.
            new WriterProcess((writerId=3, value=23, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=false,
                recoverExisting=false, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.committed;

            // The stale zone returns bearing generation 0's orphan [13] and
            // a quorum zone goes dark: recovery now depends on the returning
            // zone. Reads of generation 1 must NOT see the orphan bytes —
            // recovery re-creates zone 0 at generation 1, promotes the one
            // surviving witness's [23], and seals. Without retirement this
            // is a permanent wedge: [13] and [23] disagree at offset 0 and
            // no compatible pair exists.
            send buckets[0], eRestart;
            send buckets[1], eCrash;
            new WriterProcess((writerId=4, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=0));
            receive { case eWriterDone: (payload: (
                writerId: int, committed: bool, sealed: bool, crashed: bool
            )) { stopped = payload; } }
            assert stopped.sealed;
        }
    }
}

// A rotation racing a takeover: the active writer's rotation commit must
// fail once a recoverer has claimed a higher epoch, so no second seal
// decision can ever be committed for the segment.
machine FencedRotationDriver {
    start state Init {
        entry {
            var buckets: seq[ZonalBucket];
            var manifest: ManifestRegister;
            var i: int;
            var done: int;
            i = 0;
            while (i < 3) {
                buckets += (i, new ZonalBucket((zone=i, failures=1)));
                i = i + 1;
            }
            manifest = new ManifestRegister((failures=1,));
            new WriterProcess((writerId=1, value=81, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=false, crashBudget=1));
            new WriterProcess((writerId=2, value=-1, buckets=buckets, segBase=0,
                manifest=manifest, parent=this, shouldSeal=true,
                recoverExisting=true, crashBudget=1));
            done = 0;
            while (done < 2) {
                receive { case eWriterDone: (payload: (
                    writerId: int, committed: bool, sealed: bool, crashed: bool
                )) { done = done + 1; } }
            }
        }
    }
}
