test tcConcurrentCreators [main=ConcurrentWritersDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, ManifestSafety,
        PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { ConcurrentWritersDriver });

test tcTwoZoneSealOnlyRecovery [main=TwoZoneRecoveryDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment,
        SealAndPrefixSafety, ManifestSafety, PendingRegistrationSafety,
        DirectoryStructure, DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { TwoZoneRecoveryDriver });

test tcFinalizedGenerationRecovery [main=FinalizedGenerationRecoveryDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment,
        SealAndPrefixSafety, ManifestSafety, PendingRegistrationSafety,
        DirectoryStructure, DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { FinalizedGenerationRecoveryDriver });

test tcConditionalProgress [main=ProgressDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, ManifestSafety,
        PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement, ConditionalProgress in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { ProgressDriver });

test tcPipelinedRecordsStartupReplayAndTruncation
    [main=PipelinedRecordTruncationDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        StartupReplayAndTruncation, ManifestSafety, RotationGateSafety,
        PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { PipelinedWriter }, { StartupReplay }, { TruncationCoordinator },
        { ZonalBucket }, { ManifestRegister },
        { PipelinedRecordTruncationDriver });

test tcImmutableSealedRepair [main=SealedRepairDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { SealedRepairCoordinator }, { ZonalBucket },
        { ManifestRegister }, { SealedRepairDriver });

test tcHistoricalRecoveryRepair [main=HistoricalRecoveryRepairDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement, HistoricalRecoveryQuorum in
    (union { WriterProcess }, { HistoricalRecoveryCoordinator },
        { ZonalBucket }, { ManifestRegister },
        { HistoricalRecoveryRepairDriver });

test tcGetSizeExcludesOpenTail [main=GetSizeDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment,
        GetSizeExcludesOpenTail, ManifestSafety, PendingRegistrationSafety,
        DirectoryStructure, DirectoryEnforcement in
    (union { WriterProcess }, { GetSizeProbe }, { ZonalBucket },
        { ManifestRegister }, { GetSizeDriver });

test tcAmbiguousTailPromotion [main=AmbiguousTailPromotionDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { AmbiguousTailPromotionDriver });

test tcCorruptLaneRecovery [main=CorruptLaneRecoveryDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { CorruptLaneRecoveryDriver });

test tcRotationChainRecovery [main=RotationChainDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, RotationGateSafety, DirectoryStructure,
        PendingRegistrationSafety, DirectoryEnforcement in
    (union { PipelinedWriter }, { WriterProcess }, { ZonalBucket },
        { ManifestRegister }, { RotationChainDriver });

test tcRacingRecoveries [main=RacingRecoveriesDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { RacingRecoveriesDriver });

test tcStaleReplicaRecovery [main=StaleReplicaRecoveryDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { StaleReplicaRecoveryDriver });

test tcFencedRotation [main=FencedRotationDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { FencedRotationDriver });

test tcDirectoryLifecycle [main=DirectoryLifecycleDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        StartupReplayAndTruncation, ManifestSafety, RotationGateSafety,
        PendingRegistrationSafety, DirectoryStructure,
        DirectoryEnforcement in
    (union { DirectoryRotation }, { DirectoryCleanupCoordinator },
        { WriterProcess }, { ZonalBucket }, { ManifestRegister },
        { DirectoryLifecycleDriver });

test tcPendingSegmentRotation [main=PendingSegmentRotationDriver]:
    assert QuorumLinearizability, SingleWriterPerSegment, SealAndPrefixSafety,
        ManifestSafety, PendingRegistrationSafety,
        PendingRecoveryCompleteness, DirectoryStructure,
        DirectoryEnforcement in
    (union { PendingSegmentWriter }, { PendingChainRecovery },
        { ZonalBucket }, { ManifestRegister },
        { PendingSegmentRotationDriver });
