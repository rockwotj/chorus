package chorus.pobserve;

import QuorumModel.pobserve.PEvents;
import QuorumModel.pobserve.PMachines;
import QuorumModel.pobserve.PTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import pobserve.runtime.Monitor;
import pobserve.runtime.events.PEvent;

/** Feeds chorus-dst JSONL observations into monitors generated from Monitors.p. */
public final class ChorusPObserve {
    private static final ObjectMapper JSON = new ObjectMapper();

    private record Binding(
            String name, Monitor<?> monitor, Set<Class<? extends PEvent<?>>> eventTypes) {}

    private ChorusPObserve() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(
                    "usage: java -jar chorus-pobserve.jar TRACE.jsonl|BATCH_DIR|BATCH.manifest");
            System.exit(2);
        }

        try {
            observe(Path.of(args[0]), System.out);
        } catch (Exception failure) {
            System.err.println(failure.getMessage());
            System.exit(1);
        }
    }

    static void observe(Path input, PrintStream output) throws Exception {
        boolean batch = Files.isDirectory(input) || input.toString().endsWith(".manifest");
        List<Path> traces = batch ? batchTraces(input) : List.of(input);
        long observed = 0;
        for (Path trace : traces) {
            observed += observeTrace(trace);
        }

        if (batch) {
            output.printf("batch accepted: %d traces%n", traces.size());
        } else {
            output.printf(
                    "PObserve accepted %d protocol events from %s%n", observed, input);
        }
    }

    private static List<Path> batchTraces(Path input) throws IOException {
        List<Path> traces =
                Files.isDirectory(input) ? directoryTraces(input) : manifestTraces(input);
        if (traces.isEmpty()) {
            throw new IllegalArgumentException(input + ": batch contains no traces");
        }
        return traces;
    }

    private static List<Path> directoryTraces(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                    .sorted()
                    .toList();
        }
    }

    private static List<Path> manifestTraces(Path manifest) throws IOException {
        Path parent = manifest.toAbsolutePath().getParent();
        List<Path> traces = new ArrayList<>();
        for (String entry : Files.readAllLines(manifest)) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            Path trace = Path.of(trimmed);
            traces.add(trace.isAbsolute() ? trace : parent.resolve(trace).normalize());
        }
        return traces;
    }

    private static long observeTrace(Path trace) throws IOException {
        // A monitor instance is a state machine, not a reusable parser. A
        // certification batch is a set of independent seeded executions, so
        // carrying state across files would invent cross-seed history and make
        // acceptance depend on the arbitrary batch boundary.
        List<Binding> monitors = monitors();
        long lineNumber = 0;
        long observed = 0;
        try (BufferedReader lines = Files.newBufferedReader(trace)) {
            String line;
            while ((line = lines.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                JsonNode observation = JSON.readTree(line);
                PEvent<?> event = toPEvent(observation);
                if (event == null) {
                    continue;
                }
                observed++;
                for (Binding binding : monitors) {
                    if (!binding.eventTypes().contains(event.getClass())) {
                        continue;
                    }
                    try {
                        binding.monitor().accept(event);
                    } catch (RuntimeException failure) {
                        throw new IllegalStateException(
                                trace + ": PObserve monitor " + binding.name()
                                        + " rejected line " + lineNumber + ": " + event,
                                failure);
                    }
                }
            }
        }
        return observed;
    }

    private static List<Binding> monitors() {
        PMachines.QuorumLinearizability quorum = new PMachines.QuorumLinearizability();
        PMachines.SingleWriterPerSegment writers = new PMachines.SingleWriterPerSegment();
        PMachines.SealAndPrefixSafety seals = new PMachines.SealAndPrefixSafety();
        PMachines.StartupReplayAndTruncation replay =
                new PMachines.StartupReplayAndTruncation();
        PMachines.GetSizeExcludesOpenTail sizes = new PMachines.GetSizeExcludesOpenTail();
        PMachines.ManifestSafety manifest = new PMachines.ManifestSafety();
        PMachines.DirectoryStructure directoryStructure = new PMachines.DirectoryStructure();
        PMachines.DirectoryEnforcement directoryEnforcement =
                new PMachines.DirectoryEnforcement();
        quorum.ready();
        writers.ready();
        seals.ready();
        replay.ready();
        sizes.ready();
        manifest.ready();
        directoryStructure.ready();
        directoryEnforcement.ready();
        return List.of(
                binding("QuorumLinearizability", quorum, quorum.getEventTypes()),
                binding("SingleWriterPerSegment", writers, writers.getEventTypes()),
                binding("SealAndPrefixSafety", seals, seals.getEventTypes()),
                binding("StartupReplayAndTruncation", replay, replay.getEventTypes()),
                binding("GetSizeExcludesOpenTail", sizes, sizes.getEventTypes()),
                binding("ManifestSafety", manifest, manifest.getEventTypes()),
                binding(
                        "DirectoryStructure",
                        directoryStructure,
                        directoryStructure.getEventTypes()),
                binding(
                        "DirectoryEnforcement",
                        directoryEnforcement,
                        directoryEnforcement.getEventTypes()));
    }

    private static Binding binding(
            String name,
            Monitor<?> monitor,
            List<Class<? extends PEvent<?>>> eventTypes) {
        return new Binding(name, monitor, new HashSet<>(eventTypes));
    }

    private static PEvent<?> toPEvent(JsonNode event) {
        String name = requiredText(event, "event");
        return switch (name) {
            case "RecordFormed" -> new PEvents.eRecordFormed(writerRecord(event));
            // The trace's gen field is the per-base generation ordinal the
            // production harness assigns as it observes distinct object ids
            // at one segment base: recovery retiring an empty tail name and
            // committing a fresh id at the same base is two generations.
            case "RecordPersisted" -> new PEvents.eRecordPersisted(
                    new PTypes.PTuple_zone_wrtr_rcrd_gen(
                            required(event, "zone"),
                            required(event, "writer"),
                            record(event),
                            optionalGen(event)));
            case "CanonicalPersisted" ->
                    new PEvents.eCanonicalPersisted(zoneWriterRecord(event));
            case "RecordCommitted" -> new PEvents.eRecordCommitted(writerRecord(event));
            case "ProducerAcknowledged" -> new PEvents.eProducerAck(
                    new PTypes.PTuple_wrtr_offst(
                            required(event, "writer"),
                            required(event, "logical_offset")));
            case "RecoveryStarted" -> new PEvents.eRecoveryStarted(
                    new PTypes.PTuple_wrtr_sgmnt_gen(
                            required(event, "writer"),
                            required(event, "segment"),
                            optionalGen(event)));
            case "RecoverySelected" -> new PEvents.eRecoverySelected(writerRecord(event));
            case "RecoveryCompleted" -> new PEvents.eRecoveryCompleted(
                    new PTypes.PTuple_wrtr_sgmnt_strto_endof(
                            required(event, "writer"),
                            required(event, "segment"),
                            required(event, "logical_offset"),
                            required(event, "record_end")));
            case "DirectoryAdopted" -> new PEvents.eDirectoryAdopted(
                    new PTypes.PTuple_wrtr_drctr_entry_entry_endof_tlbs_crrnt_crrnt_trunc(
                            required(event, "writer"),
                            new PTypes.PTuple_base_id(
                                    required(event, "segment"),
                                    required(event, "segment_id")),
                            required(event, "directory_index"),
                            required(event, "directory_len"),
                            required(event, "record_end"),
                            required(event, "tail_base"),
                            optional(event, "seal_base", -1),
                            optional(event, "current_seal_id", -1),
                            required(event, "truncation_floor")));
            case "SegmentCreated" -> new PEvents.eSegmentCreated(
                    new PTypes.PTuple_zone_wrtr_epoch_sgmnt_gen(
                            required(event, "zone"),
                            required(event, "writer"),
                            required(event, "epoch"),
                            required(event, "segment"),
                            optionalGen(event)));
            case "SegmentOpened" -> new PEvents.eSegmentOpened(
                    new PTypes.PTuple_sgmnt_wrtr_epoch_gen(
                            required(event, "segment"),
                            required(event, "writer"),
                            required(event, "epoch"),
                            optionalGen(event)));
            case "SegmentFinalized" -> new PEvents.eSegmentFinalized(
                    new PTypes.PTuple_zone_sgmnt_vlden(
                            required(event, "zone"),
                            required(event, "segment"),
                            required(event, "record_end")));
            case "SealQuorumEnforced" -> new PEvents.eSealQuorumEnforced(
                    new PTypes.PTuple_sgmnt_sgmnt_endof(
                            required(event, "segment"),
                            required(event, "segment_id"),
                            required(event, "record_end")));
            case "SegmentSealed" -> new PEvents.eSegmentSealed(
                    new PTypes.PTuple_sgmnt_endof(
                            required(event, "segment"), required(event, "record_end")));
            case "RotationGateReleased" -> new PEvents.eRotationGateReleased(
                    new PTypes.PTuple_sgmnt_sgmnt_endof(
                            required(event, "segment"),
                            required(event, "segment_id"),
                            required(event, "record_end")));
            case "SealedCopyRepaired" -> new PEvents.eSealedCopyRepaired(
                    new PTypes.PTuple_zone_sgmnt_endof_rcrdc(
                            required(event, "zone"),
                            required(event, "segment"),
                            required(event, "record_end"),
                            required(event, "record_end") - required(event, "segment") + 1));
            case "ReplayOpened" -> new PEvents.eReplayOpened(
                    new PTypes.PTuple_rdr_strto_endof(
                            required(event, "reader"),
                            required(event, "logical_offset"),
                            required(event, "record_end")));
            case "ReplayRecord" -> new PEvents.eReplayRecord(
                    new PTypes.PTuple_rdr_offst(
                            required(event, "reader"),
                            required(event, "logical_offset")));
            case "ReplayClosed" -> new PEvents.eReplayClosed(required(event, "reader"));
            case "TruncationProposed" ->
                    new PEvents.eTruncationProposed(required(event, "truncation_floor"));
            case "EpochClaimed" -> new PEvents.eEpochClaimed(
                    new PTypes.PTuple_epoch_wrtr(
                            required(event, "epoch"), required(event, "writer")));
            case "ViewCommitted" -> new PEvents.eViewCommitted(
                    new PTypes.PTuple_epoch_tlbs_slbs_slend_slsm(
                            required(event, "epoch"),
                            required(event, "logical_offset"),
                            required(event, "segment"),
                            required(event, "record_end"),
                            required(event, "value")));
            case "FloorCommitted" ->
                    new PEvents.eFloorCommitted(required(event, "truncation_floor"));
            case "SegmentDeleted" -> new PEvents.eSegmentDeleted(
                    new PTypes.PTuple_zone_sgmnt_endof_floor(
                            required(event, "zone"),
                            required(event, "segment"),
                            required(event, "record_end"),
                            required(event, "truncation_floor")));
            case "GetSizeObserved" -> new PEvents.eGetSizeObserved(
                    new PTypes.PTuple_zone_size_fnlzd(
                            required(event, "zone"),
                            required(event, "reported_size"),
                            requiredBoolean(event, "finalized")));
            case "SegmentCreateAttempt",
                    "SegmentCreateRejected",
                    "ProducerSpike",
                    "ZoneCrash",
                    "ZoneRestart",
                    "DiskCorrupted",
                    "WriterCrash",
                    "WriterRestart",
                    "RpcDropped",
                    "RpcUnavailable",
                    "RpcDeadlineExceeded" -> null;
            default -> throw new IllegalArgumentException(
                    "trace event has no PObserve mapping or explicit exclusion: " + name);
        };
    }

    private static PTypes.PTuple_wrtr_rcrd writerRecord(JsonNode event) {
        return new PTypes.PTuple_wrtr_rcrd(required(event, "writer"), record(event));
    }

    private static PTypes.PTuple_zone_wrtr_rcrd zoneWriterRecord(JsonNode event) {
        return new PTypes.PTuple_zone_wrtr_rcrd(
                required(event, "zone"), required(event, "writer"), record(event));
    }

    private static PTypes.PTuple_offst_value_sgmnt record(JsonNode event) {
        return new PTypes.PTuple_offst_value_sgmnt(
                required(event, "logical_offset"),
                required(event, "value"),
                required(event, "segment"));
    }

    /** The trace's generation field; absent or null means generation 0. */
    private static long optionalGen(JsonNode event) {
        JsonNode value = event.get("gen");
        if (value == null || value.isNull()) {
            return 0;
        }
        if (!value.isIntegralNumber()) {
            throw new IllegalArgumentException(
                    requiredText(event, "event") + " carries a non-integral gen");
        }
        return value.bigIntegerValue().longValue();
    }

    private static long required(JsonNode event, String field) {
        JsonNode value = event.get(field);
        if (value == null || value.isNull() || !value.isIntegralNumber()) {
            throw new IllegalArgumentException(
                    requiredText(event, "event") + " requires integral field " + field);
        }
        return value.bigIntegerValue().longValue();
    }

    private static long optional(JsonNode event, String field, long defaultValue) {
        JsonNode value = event.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        if (!value.isIntegralNumber()) {
            throw new IllegalArgumentException(
                    requiredText(event, "event") + " carries non-integral field " + field);
        }
        return value.bigIntegerValue().longValue();
    }

    private static boolean requiredBoolean(JsonNode event, String field) {
        JsonNode value = event.get(field);
        if (value == null || !value.isBoolean()) {
            throw new IllegalArgumentException(
                    requiredText(event, "event") + " requires boolean field " + field);
        }
        return value.booleanValue();
    }

    private static String requiredText(JsonNode event, String field) {
        JsonNode value = event.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("trace event requires text field " + field);
        }
        return value.textValue();
    }
}
