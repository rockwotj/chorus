// Recovery retains a hot directory and archive root from one manifest version.
// Publication and deletion race a later recovery CAS, whose retry refreshes the
// live register but must not replace the adopted replay root.
machine ArchiveRecoverySnapshot {
    var manifest: ManifestRegister;
    var view: tManifestRecord;
    var version: int;
    var pages: map[int, seq[int]];
    var hotRecords: set[int];
    var durable: set[int];
    var checkpoint: int;
    var emitted: seq[int];

    start state Running {
        entry {
            var response: tManifestCasResponse;
            var adopted: tManifestRecord;
            var adoptedVersion: int;
            var next: tManifestRecord;
            var replayRoot: int;
            var index: int;
            var value: int;
            manifest = new ManifestRegister((failures=0,));
            send manifest, eArchiveEnable, (caller=this,);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            view = response.rec;
            version = response.metagen;
            while (view.tailBase < 4) {
                next = view;
                hotRecords += (view.tailBase);
                next.directory += (sizeof(next.directory), (base=view.tailBase, id=100 + view.tailBase));
                next.sealBase = view.tailBase;
                next.sealId = 100 + view.tailBase;
                next.tailBase = view.tailBase + 1;
                next.sealEnd = next.tailBase;
                next.sealSum = next.sealId;
                send manifest, eManifestCas, (caller=this, expMetagen=version, rec=next);
                receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
                assert response.status == STATUS_OK;
                view = response.rec;
                version = response.metagen;
            }
            // Exercise both no archive root and an existing archived prefix.
            if ($) { PublishAndDelete(); }
            checkpoint = 0;
            if ($) { checkpoint = checkpoint + 1; }
            if ($) { checkpoint = checkpoint + 2; }
            adopted = view;
            adoptedVersion = version;
            PublishAndDelete();
            // A stale recovery CAS conflicts, then succeeds after refreshing.
            next = adopted;
            next.epoch = next.epoch + 1;
            next.owner = next.epoch;
            send manifest, eManifestCas, (caller=this, expMetagen=adoptedVersion, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            assert response.status == STATUS_FENCED;
            view = response.rec;
            version = response.metagen;
            next = view;
            next.epoch = next.epoch + 1;
            next.owner = next.epoch;
            send manifest, eManifestCas, (caller=this, expMetagen=version, rec=next);
            receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
            assert response.status == STATUS_OK;
            view = response.rec;
            assert view.archive.end > adopted.archive.end;
            replayRoot = adopted.archive.root;
            if (replayRoot != 0) {
                index = 0;
                while (index < sizeof(pages[replayRoot])) {
                    value = pages[replayRoot][index];
                    assert value in durable;
                    Emit(value);
                    index = index + 1;
                }
            }
            index = 0;
            while (index < sizeof(adopted.directory)) {
                value = adopted.directory[index].base;
                // A deleted captured hot entry resolves through the newer root.
                if (!(value in hotRecords)) {
                    assert value in durable && view.archive.root in pages &&
                        pages[view.archive.root][value] == value;
                }
                Emit(value);
                index = index + 1;
            }
            assert sizeof(emitted) == adopted.tailBase - checkpoint,
                "recovery omitted records from its fixed replay range";
        }
    }

    fun Emit(value: int) {
        if (value >= checkpoint) {
            assert value == checkpoint + sizeof(emitted),
                "recovery skipped or duplicated a record across archive/hot snapshots";
            emitted += (sizeof(emitted), value);
        }
    }

    fun PublishAndDelete() {
        var response: tManifestCasResponse;
        var page: seq[int];
        var value: int;
        var root: int;
        value = view.directory[0].base;
        root = view.directory[1].base;
        if (view.archive.root != 0) { page = pages[view.archive.root]; }
        page += (sizeof(page), value);
        durable += (value);
        pages[root] = page;
        send manifest, eArchivePublish, (caller=this, expMetagen=version,
            previous=view.archive.root, root=root, segmentEntry=view.directory[0], end=root);
        receive { case eManifestCasResponse: (r: tManifestCasResponse) { response = r; } }
        assert response.status == STATUS_OK;
        view = response.rec;
        version = response.metagen;
        hotRecords -= (value);
    }
}
