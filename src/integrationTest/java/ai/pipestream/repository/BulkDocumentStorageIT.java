package ai.pipestream.repository;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.*;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import ai.pipestream.test.support.S3TestResource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bulk document storage integration test.
 * <p>
 * Loads 100+ pre-parsed PipeDoc protobuf files, stores them via gRPC,
 * retrieves each one, and verifies SHA-256 checksum integrity on
 * the round-tripped protobuf bytes.
 * <p>
 * Runs with {@code redis-buffered} mode enabled to exercise the Redis
 * cache layer. Measures and reports timing for store and retrieve operations.
 */
@QuarkusIntegrationTest
@TestProfile(BulkDocumentStorageIT.RedisEnabledProfile.class)
@QuarkusTestResource(S3TestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BulkDocumentStorageIT {

    private static final Logger LOG = Logger.getLogger(BulkDocumentStorageIT.class);
    private static final String VALID_ACCOUNT = "valid-account";
    private static final String DATASOURCE = "ds-bulk-test";
    private static final String CONNECTOR = "conn-bulk-test";
    private static final Path SAMPLE_DOCS_DIR = Path.of("/work/sample-documents/sample-documents/parser-pipedoc-parsed/src/main/resources");

    @TestHTTPResource
    URL url;

    private ManagedChannel channel;
    private PipeDocServiceGrpc.PipeDocServiceBlockingStub stub;

    /** Stored document metadata: docId → nodeId */
    private static final Map<String, String> storedDocs = new LinkedHashMap<>();
    /** Original protobuf bytes SHA-256: docId → checksum */
    private static final Map<String, String> originalChecksums = new LinkedHashMap<>();
    /** Store timing: docId → nanoseconds */
    private static final Map<String, Long> storeTimes = new LinkedHashMap<>();
    /** Retrieve timing: docId → nanoseconds */
    private static final Map<String, Long> retrieveTimes = new LinkedHashMap<>();

    public static class RedisEnabledProfile extends RepositoryIntegrationTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>(super.getConfigOverrides());
            config.put("repo.cache.storage-mode", "redis-buffered");
            return config;
        }
    }

    @BeforeEach
    void setUp() {
        channel = ManagedChannelBuilder
                .forAddress(url.getHost(), url.getPort())
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .build();
        stub = PipeDocServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(1)
    void bulkStore_allSampleDocuments() throws IOException {
        List<Path> sampleFiles = listSampleFiles();
        assertThat(sampleFiles)
                .as("Should find sample PipeDoc protobuf files in %s", SAMPLE_DOCS_DIR)
                .hasSizeGreaterThanOrEqualTo(50);

        LOG.infof("=== Bulk Store: %d documents (redis-buffered mode) ===", sampleFiles.size());
        int stored = 0;
        int failed = 0;

        for (Path file : sampleFiles) {
            byte[] rawBytes = Files.readAllBytes(file);
            String sha256 = sha256(rawBytes);

            PipeDoc originalDoc;
            try {
                originalDoc = PipeDoc.parseFrom(rawBytes);
            } catch (Exception e) {
                LOG.warnf("Skipping %s — not a valid PipeDoc protobuf: %s", file.getFileName(), e.getMessage());
                failed++;
                continue;
            }

            // Assign a unique docId to avoid collisions across test runs
            String docId = "bulk-" + file.getFileName().toString().replace(".pb", "") + "-" + System.nanoTime();
            PipeDoc docWithOwnership = originalDoc.toBuilder()
                    .setDocId(docId)
                    .setOwnership(OwnershipContext.newBuilder()
                            .setAccountId(VALID_ACCOUNT)
                            .setDatasourceId(DATASOURCE)
                            .setConnectorId(CONNECTOR)
                            .build())
                    .build();

            // Store the modified doc's bytes checksum (what we actually send)
            byte[] storedBytes = docWithOwnership.toByteArray();
            originalChecksums.put(docId, sha256(storedBytes));

            long startNs = System.nanoTime();
            try {
                SavePipeDocResponse response = stub.savePipeDoc(
                        SavePipeDocRequest.newBuilder()
                                .setPipedoc(docWithOwnership)
                                .setDrive("default")
                                .setUseDatasourceId(true)
                                .build());

                long elapsedNs = System.nanoTime() - startNs;
                storeTimes.put(docId, elapsedNs);
                storedDocs.put(docId, response.getNodeId());
                stored++;
            } catch (Exception e) {
                LOG.warnf(e, "Failed to store %s: %s", file.getFileName(), e.getMessage());
                failed++;
            }
        }

        LOG.infof("=== Bulk Store Complete: %d stored, %d failed ===", stored, failed);
        assertThat(stored)
                .as("At least 50 documents should store successfully")
                .isGreaterThanOrEqualTo(50);

        // Report timing stats
        LongSummaryStatistics storeStats = storeTimes.values().stream()
                .mapToLong(Long::longValue).summaryStatistics();
        LOG.infof("Store timing: min=%.1fms, avg=%.1fms, max=%.1fms, total=%.1fs (count=%d)",
                storeStats.getMin() / 1_000_000.0,
                storeStats.getAverage() / 1_000_000.0,
                storeStats.getMax() / 1_000_000.0,
                storeStats.getSum() / 1_000_000_000.0,
                storeStats.getCount());
    }

    @Test
    @Order(2)
    void bulkRetrieve_allStoredDocuments_checksumMatch() {
        assertThat(storedDocs)
                .as("storedDocs should be populated by prior test")
                .isNotEmpty();

        LOG.infof("=== Bulk Retrieve: %d documents ===", storedDocs.size());
        int matched = 0;
        int mismatched = 0;
        int missing = 0;

        for (Map.Entry<String, String> entry : storedDocs.entrySet()) {
            String docId = entry.getKey();
            String nodeId = entry.getValue();

            long startNs = System.nanoTime();
            try {
                GetPipeDocResponse response = stub.getPipeDoc(
                        GetPipeDocRequest.newBuilder().setNodeId(nodeId).build());
                long elapsedNs = System.nanoTime() - startNs;
                retrieveTimes.put(docId, elapsedNs);

                if (!response.hasPipedoc()) {
                    LOG.warnf("Missing document on retrieve: docId=%s, nodeId=%s", docId, nodeId);
                    missing++;
                    continue;
                }

                // Verify the retrieved doc has the same docId
                PipeDoc retrieved = response.getPipedoc();
                assertThat(retrieved.getDocId())
                        .as("Retrieved docId should match stored docId for nodeId=%s", nodeId)
                        .isEqualTo(docId);

                // SHA-256 checksum of the retrieved protobuf bytes
                String retrievedChecksum = sha256(retrieved.toByteArray());
                String originalChecksum = originalChecksums.get(docId);

                if (retrievedChecksum.equals(originalChecksum)) {
                    matched++;
                } else {
                    LOG.warnf("Checksum mismatch: docId=%s, expected=%s, got=%s",
                            docId, originalChecksum, retrievedChecksum);
                    mismatched++;
                }
            } catch (Exception e) {
                LOG.warnf(e, "Failed to retrieve docId=%s, nodeId=%s: %s", docId, nodeId, e.getMessage());
                missing++;
            }
        }

        LOG.infof("=== Bulk Retrieve Complete: %d matched, %d mismatched, %d missing ===",
                matched, mismatched, missing);

        // Report timing stats
        LongSummaryStatistics retrieveStats = retrieveTimes.values().stream()
                .mapToLong(Long::longValue).summaryStatistics();
        LOG.infof("Retrieve timing: min=%.1fms, avg=%.1fms, max=%.1fms, total=%.1fs (count=%d)",
                retrieveStats.getMin() / 1_000_000.0,
                retrieveStats.getAverage() / 1_000_000.0,
                retrieveStats.getMax() / 1_000_000.0,
                retrieveStats.getSum() / 1_000_000_000.0,
                retrieveStats.getCount());

        assertThat(mismatched)
                .as("No documents should have checksum mismatches")
                .isZero();
        assertThat(missing)
                .as("No documents should be missing on retrieval")
                .isZero();
        assertThat(matched)
                .as("All stored documents should match on checksum")
                .isEqualTo(storedDocs.size());
    }

    @Test
    @Order(3)
    void timingSummary() {
        if (storeTimes.isEmpty() || retrieveTimes.isEmpty()) {
            LOG.warn("Skipping timing summary — no data from prior tests");
            return;
        }

        LongSummaryStatistics storeStats = storeTimes.values().stream()
                .mapToLong(Long::longValue).summaryStatistics();
        LongSummaryStatistics retrieveStats = retrieveTimes.values().stream()
                .mapToLong(Long::longValue).summaryStatistics();

        LOG.infof("╔══════════════════════════════════════════════════════════╗");
        LOG.infof("║          BULK DOCUMENT STORAGE TIMING REPORT            ║");
        LOG.infof("╠══════════════════════════════════════════════════════════╣");
        LOG.infof("║  Documents: %-43d ║", storedDocs.size());
        LOG.infof("║  Mode: %-48s ║", "redis-buffered");
        LOG.infof("╠══════════════════════════════════════════════════════════╣");
        LOG.infof("║  STORE                                                  ║");
        LOG.infof("║    Min:   %8.2f ms                                   ║", storeStats.getMin() / 1e6);
        LOG.infof("║    Avg:   %8.2f ms                                   ║", storeStats.getAverage() / 1e6);
        LOG.infof("║    Max:   %8.2f ms                                   ║", storeStats.getMax() / 1e6);
        LOG.infof("║    Total: %8.2f s                                    ║", storeStats.getSum() / 1e9);
        LOG.infof("╠══════════════════════════════════════════════════════════╣");
        LOG.infof("║  RETRIEVE (Redis L1 + S3 L2)                            ║");
        LOG.infof("║    Min:   %8.2f ms                                   ║", retrieveStats.getMin() / 1e6);
        LOG.infof("║    Avg:   %8.2f ms                                   ║", retrieveStats.getAverage() / 1e6);
        LOG.infof("║    Max:   %8.2f ms                                   ║", retrieveStats.getMax() / 1e6);
        LOG.infof("║    Total: %8.2f s                                    ║", retrieveStats.getSum() / 1e9);
        LOG.infof("╚══════════════════════════════════════════════════════════╝");
    }

    private List<Path> listSampleFiles() throws IOException {
        if (!Files.isDirectory(SAMPLE_DOCS_DIR)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.list(SAMPLE_DOCS_DIR)) {
            return walk.filter(p -> p.toString().endsWith(".pb"))
                    .sorted()
                    .toList();
        }
    }

    private static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
