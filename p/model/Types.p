enum tStatus {
    STATUS_OK,
    STATUS_TRANSIENT,
    STATUS_FENCED,
    STATUS_FINALIZED,
    STATUS_EXISTS,
    STATUS_NOT_FOUND
}

// Durable records deliberately contain no writer, epoch, or producer identity.
type tRecord = (
    offset: int,
    value: int,
    segment: int
);

// `gen` is the manifest's tail generation, standing in for the
// implementation's per-incarnation object name ({epoch}-{seq}): two
// generations at one base are two distinct objects. Requests address one
// name; a generation mismatch behaves as if the name does not exist.
// gen == -1 stands for a name taken from a committed catalog entry rather
// than the manifest's tail id — sealed-segment operations are name-exact
// in the implementation, so the model treats them as always matching.
type tCreateRequest = (
    caller: machine, writerId: int, epoch: int, segment: int, gen: int
);
type tCreateResponse = (zone: int, status: tStatus);
type tTakeoverRequest = (
    caller: machine, writerId: int, epoch: int, segment: int, gen: int
);
type tTakeoverResponse = (
    zone: int, status: tStatus, persistedSize: int
);
type tReadRequest = (caller: machine, segment: int, gen: int);
type tReadResponse = (
    zone: int,
    status: tStatus,
    records: seq[tRecord],
    finalized: bool
);
type tAppendRequest = (
    caller: machine, writerId: int, epoch: int, gen: int, record: tRecord
);
type tAppendResponse = (
    zone: int, status: tStatus, persistedSize: int, offset: int
);
type tReplaceRequest = (
    caller: machine,
    writerId: int,
    epoch: int,
    segment: int,
    gen: int,
    records: seq[tRecord]
);
type tReplaceResponse = (zone: int, status: tStatus, persistedSize: int);
type tFinalizeRequest = (
    caller: machine,
    writerId: int,
    epoch: int,
    segment: int,
    gen: int,
    validEnd: int
);
type tFinalizeResponse = (zone: int, status: tStatus, validEnd: int);
type tRepairRequest = (
    caller: machine,
    segment: int,
    records: seq[tRecord],
    validEnd: int
);
type tRepairResponse = (zone: int, status: tStatus);
type tGetRequest = (caller: machine);
type tGetResponse = (
    zone: int,
    status: tStatus,
    reportedSize: int,
    finalized: bool
);
type tDeleteRequest = (
    caller: machine, segment: int, endOffset: int, floor: int
);
type tDeleteResponse = (zone: int, status: tStatus, segment: int);
// Discard a retired tail name: fire-and-forget deletion of an empty,
// never-acknowledged witness, sent only after the fresh tail generation
// is committed through the register (decision before enforcement).
type tDiscardRequest = (segment: int, gen: int);

type tWriterConfig = (
    writerId: int,
    value: int,
    buckets: seq[ZonalBucket],
    // base record index of the segment `buckets` holds; the manifest decides
    // whether recovery treats it as the active tail or a sealed predecessor
    segBase: int,
    manifest: ManifestRegister,
    parent: machine,
    shouldSeal: bool,
    recoverExisting: bool,
    crashBudget: int
);

// One authoritative sealed-chain directory entry. The bounded model uses a
// small integer as the opaque production object id; `base` alone is not the
// identity because a tail name can be retired and replaced at the same base.
type tDirectoryEntry = (
    base: int,
    id: int
);

// The regional manifest register. The digest surrogate stands for the
// implementation's SHA-256 of the sealed bytes: the model compares it for
// equality only.
// tailGen is the opaque id/generation surrogate for the tail object's name.
// `pending` is one optional opaque id in the same namespace. -1 means absent.
// It deliberately carries no base: recovery derives the pending base from the
// quorum-observed committed size of the tail. Recovery that finds the tail
// empty or absent never reuses the name: it commits tailGen+1 through the
// register (the implementation mints a fresh successor id via commit_view), so
// bytes under a retired name can never rejoin the log.
type tManifestRecord = (
    epoch: int,
    owner: int,
    tailBase: int,
    tailGen: int,
    pending: int,
    sealBase: int,
    sealId: int,
    sealEnd: int,
    sealSum: int,
    trunc: int,
    directory: seq[tDirectoryEntry]
);
type tManifestReadRequest = (caller: machine);
type tManifestReadResponse = (
    status: tStatus, metagen: int, rec: tManifestRecord
);
type tManifestCasRequest = (
    caller: machine, expMetagen: int, rec: tManifestRecord
);
type tManifestCasResponse = (
    status: tStatus, metagen: int, rec: tManifestRecord
);
type tDirectoryRemoveRequest = (
    caller: machine,
    expMetagen: int,
    directoryEntry: tDirectoryEntry,
    floor: int,
    absentZones: set[int]
);
type tDirectoryRemoveResponse = (
    status: tStatus, metagen: int, rec: tManifestRecord
);

event eCreateSegment: tCreateRequest;
event eCreateResponse: tCreateResponse;
event eTakeoverStream: tTakeoverRequest;
event eTakeoverResponse: tTakeoverResponse;
event eRead: tReadRequest;
event eReadResponse: tReadResponse;
event eAppend: tAppendRequest;
event eAppendResponse: tAppendResponse;
event eReplacePrefix: tReplaceRequest;
event eReplaceResponse: tReplaceResponse;
event eFinalize: tFinalizeRequest;
event eFinalizeResponse: tFinalizeResponse;
event eRepairSealed: tRepairRequest;
event eRepairResponse: tRepairResponse;
event eGetObject: tGetRequest;
event eGetResponse: tGetResponse;
event eDeleteSegment: tDeleteRequest;
event eDeleteResponse: tDeleteResponse;
event eDiscardTail: tDiscardRequest;
event eCrash;
event eRestart;
// Fault injection: rot the stored record at a global offset (CRC failure).
// Readers decode-stop there; physical sizes are unchanged.
event eCorruptRecord: (offset: int);
event eWriterDone: (
    writerId: int, committed: bool, sealed: bool, crashed: bool
);

event eManifestRead: tManifestReadRequest;
event eManifestReadResponse: tManifestReadResponse;
event eManifestCas: tManifestCasRequest;
event eManifestCasResponse: tManifestCasResponse;
event eDirectoryRemove: tDirectoryRemoveRequest;
event eDirectoryRemoveResponse: tDirectoryRemoveResponse;

event eSegmentCreated: (
    zone: int, writerId: int, epoch: int, segment: int, gen: int
);
event eStreamTakenOver: (
    zone: int, writerId: int, epoch: int, segment: int
);
event eRecordPersisted: (
    zone: int, writerId: int, record: tRecord, gen: int
);
event eCanonicalPersisted: (zone: int, writerId: int, record: tRecord);
event eRecordFormed: (writerId: int, record: tRecord);
event eRecordCommitted: (writerId: int, record: tRecord);
event eProducerAck: (writerId: int, offset: int);
// Model-only acknowledgment carrying the opaque segment id. Production's
// public acknowledgment remains eProducerAck; this event lets the checker
// state the preregistration invariant without confusing an id with a base.
event eRecordAcknowledged: (
    writerId: int, record: tRecord, segmentId: int
);
event eRecoveryStarted: (writerId: int, segment: int, gen: int);
event eRecoverySelected: (writerId: int, record: tRecord);
event eRecoveryCompleted: (
    writerId: int,
    segment: int,
    startOffset: int,
    endOffset: int
);
event eDirectoryAdopted: (
    writerId: int,
    directoryEntry: tDirectoryEntry,
    entryIndex: int,
    entryCount: int,
    endOffset: int,
    tailBase: int,
    currentSealBase: int,
    currentSealId: int,
    trunc: int
);
event eDirectoryReplayed: (
    writerId: int, directoryEntry: tDirectoryEntry, endOffset: int
);
event eSegmentFinalized: (zone: int, segment: int, validEnd: int);
event eSealQuorumEnforced: (
    segment: int, segmentId: int, endOffset: int
);
event eSealedCopyRepaired: (
    zone: int, segment: int, endOffset: int, recordCount: int
);
// Model-only startup-repair observation. `checksum` is the equality-only
// surrogate for the CRC32C committed in the production directory entry.
event eHistoricalRecoveryReady: (
    segment: int, checksum: int, healthyZones: set[int]
);
event eSegmentSealed: (segment: int, endOffset: int);
event eRotationGateReleased: (
    segment: int, segmentId: int, endOffset: int
);
event eSegmentOpened: (segment: int, writerId: int, epoch: int, gen: int);
event eTruncationProposed: int;
event eEpochClaimed: (epoch: int, writerId: int);
event eViewCommitted: (
    epoch: int, tailBase: int, sealBase: int, sealEnd: int, sealSum: int
);
// Model-only register-linearized observations. Production checks the
// single-snapshot directory shape through eDirectoryAdopted instead.
event eManifestCommitted: tManifestRecord;
event eDirectoryEntryRemoved: (
    directoryEntry: tDirectoryEntry,
    endOffset: int,
    floor: int,
    absentZones: set[int],
    rec: tManifestRecord
);
// Announced by the register at the linearization point of a CAS that bumps
// tailGen, so observation order matches commit order exactly (a writer-side
// announce could be reordered past the next claim, or lost to an ambiguous
// CAS whose issuer was fenced before confirming).
event eTailRetired: (
    epoch: int, tailBase: int, oldGen: int, newGen: int
);
event eFloorCommitted: int;
event eSegmentDeleted: (
    zone: int, segment: int, endOffset: int, floor: int
);
event eReplayOpened: (reader: int, startOffset: int, endOffset: int);
event eReplayRecord: (reader: int, offset: int);
event eReplayClosed: int;
event eGetSizeObserved: (zone: int, size: int, finalized: bool);
event eReplayDone;
event eProgressRequested: int;
event eProgressCompleted: int;
event eTruncationDone;
event eRepairDone;
event ePendingWriterReady;
event ePendingWriterContinue;
event ePendingWriterDone: (
    committedEnd: int,
    maintenanceLanded: bool,
    pendingExhausted: bool
);
event ePendingRecoveryCompleted: (
    writerId: int,
    directoryEnd: int,
    recoveredEnd: int,
    pendingExhausted: bool
);
