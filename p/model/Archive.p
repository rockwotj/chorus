// The existing quorum model establishes canonical sealed bytes and the
// finalization gate. This composition models archival of those bytes through
// the real ManifestRegister transitions. Eight one-record seals bound history;
// two independently cached proposals represent competing/restarted workers.
// Immutable pages stand for the transitive closure of a paged catalog root.
type tArchiveProposal = (valid: bool, version: int, previous: int,
    root: int, segmentEntry: tDirectoryEntry, end: int);
event eArchiveStep;

machine ArchiveLifecycle {
    var manifest: ManifestRegister;
    var view: tManifestRecord;
    var version: int;
    var proposals: seq[tArchiveProposal];
    var durableData: set[int];
    var pages: map[int, seq[int]];
    var hotCopies: map[int, set[int]];
    var finalized: set[int];
    var readerSnapshot: tManifestRecord;
    var readerNext: int;
    var readerDropped: bool;
    var emitted: seq[int];
    var steps: int;

    start state Running {
        entry {
            var response: tManifestCasResponse;
            manifest = new ManifestRegister((failures=8,));
            send manifest, eArchiveEnable, (caller=this,);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            view = response.rec;
            version = response.metagen;
            proposals += (0, default(tArchiveProposal));
            proposals += (1, default(tArchiveProposal));
            readerSnapshot = view;
            send this, eArchiveStep;
        }
        on eArchiveStep do Step;
    }

    fun Refresh() {
        var response: tManifestReadResponse;
        send manifest, eManifestRead, (caller=this,);
        receive { case eManifestReadResponse: (r: tManifestReadResponse) { response = r; } }
        if (response.status == STATUS_OK) {
            view = response.rec;
            version = response.metagen;
        }
    }

    fun Step() {
        var op: int;
        var slot: int;
        var zone: int;
        var index: int;
        var next: tManifestRecord;
        var proposal: tArchiveProposal;
        var page: seq[int];
        var copies: set[int];
        var response: tManifestCasResponse;
        var canRead: bool;
        Refresh();
        op = 0;
        if ($) { op = op + 1; }
        if ($) { op = op + 2; }
        if ($) { op = op + 4; }
        slot = 0;
        if ($) { slot = 1; }
        proposal = proposals[slot];
        if (op == 0) {
            if (view.tailBase < 8 && sizeof(view.directory) < 4) {
                next = view;
                if (next.sealId >= 0) { finalized += (next.sealId); }
                copies = default(set[int]);
                copies += (0); copies += (1); copies += (2);
                hotCopies[view.tailBase] = copies;
                next.directory += (sizeof(next.directory), (base=view.tailBase, id=100 + view.tailBase));
                next.sealBase = view.tailBase;
                next.sealId = 100 + view.tailBase;
                next.tailBase = view.tailBase + 1;
                next.sealEnd = next.tailBase;
                next.sealSum = next.sealId;
                send manifest, eManifestCas, (caller=this, expMetagen=version, rec=next);
                receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            }
        } else if (op == 1) {
            if (sizeof(view.directory) >= 2) {
                proposals[slot] = (valid=true, version=version,
                    previous=view.archive.root, root=view.directory[1].base,
                    segmentEntry=view.directory[0], end=view.directory[1].base);
            }
        } else if (op == 2) {
            // Request loss leaves no data. Response loss can leave durable data
            // without advancing the worker; a retry finds identical content.
            if (proposal.valid && proposal.segmentEntry.id in finalized && $) {
                durableData += (proposal.segmentEntry.base);
            }
        } else if (op == 3) {
            if (proposal.valid && proposal.segmentEntry.base in durableData &&
                (proposal.previous == 0 || proposal.previous in pages)) {
                page = default(seq[int]);
                if (proposal.previous != 0) { page = pages[proposal.previous]; }
                page += (sizeof(page), proposal.segmentEntry.base);
                if (proposal.root in pages) {
                    assert pages[proposal.root] == page, "immutable catalog root overwritten";
                } else if ($) { pages[proposal.root] = page; }
            }
        } else if (op == 4) {
            if (proposal.valid && proposal.root in pages) {
                send manifest, eArchivePublish, (caller=this,
                    expMetagen=proposal.version, previous=proposal.previous,
                    root=proposal.root, segmentEntry=proposal.segmentEntry, end=proposal.end);
                receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
                // A worker can crash after the CAS, including after a lost
                // response. Durable data/pages/register are not rolled back.
                if ($) { proposals[slot] = default(tArchiveProposal); }
            }
        } else if (op == 5) {
            zone = 0;
            if ($) { zone = 1; } else if ($) { zone = 2; }
            index = 0;
            while (index < view.archive.end) {
                if (zone in hotCopies[index] && $) {
                    assert view.archive.root in pages && index in durableData,
                        "zonal deletion preceded durable archive publication";
                    copies = hotCopies[index]; copies -= (zone); hotCopies[index] = copies;
                    break;
                }
                index = index + 1;
            }
        } else if (op == 6) {
            if ($) { readerSnapshot = view; }
            if (!readerDropped && readerNext < readerSnapshot.tailBase) {
                canRead = sizeof(hotCopies[readerNext]) > 0;
                if (!canRead && view.archive.root in pages &&
                    readerNext < view.archive.end) {
                    assert readerNext in durableData, "archive fallback lost a record";
                    canRead = true;
                }
                if (canRead) {
                    emitted += (sizeof(emitted), readerNext);
                    readerNext = readerNext + 1;
                }
            }
            if ($ && sizeof(emitted) > 0) { readerDropped = true; }
        } else {
            // Checkpoint/epoch mutations race cached archival proposals. They
            // must preserve the root. Truncation does not delete archive data.
            next = view;
            if ($) { next.trunc = view.tailBase; }
            else { next.epoch = view.epoch + 1; next.owner = next.epoch; }
            send manifest, eManifestCas, (caller=this, expMetagen=version, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            if ($) { proposals[slot] = default(tArchiveProposal); }
        }
        // Strong snapshots can only lag on injected read failures; the safety
        // assertions apply to every successfully observed register state.
        Refresh();
        if (view.archive.root != 0) {
            assert view.archive.root in pages, "published catalog is missing";
            assert sizeof(pages[view.archive.root]) == view.archive.end,
                "published archive contains a hole or an overlapping range";
            index = 0;
            while (index < view.archive.end) {
                assert pages[view.archive.root][index] == index && index in durableData,
                    "published catalog names noncanonical or missing bytes";
                index = index + 1;
            }
            assert sizeof(view.directory) > 0 &&
                view.directory[0].base == view.archive.end,
                "archive and hot directory do not join";
            assert view.archive.end <= view.sealBase,
                "archival evicted the latest finalization witness";
        }
        index = 0;
        while (index < view.tailBase) {
            assert sizeof(hotCopies[index]) > 0 ||
                (index < view.archive.end && index in durableData),
                "recovery lost a committed record";
            index = index + 1;
        }
        index = 0;
        while (index < sizeof(emitted)) {
            assert emitted[index] == index, "reader skipped or duplicated a record";
            index = index + 1;
        }
        assert readerNext == sizeof(emitted), "reader cursor differs from delivered prefix";
        steps = steps + 1;
        if (steps < 200) { send this, eArchiveStep; }
    }
}
