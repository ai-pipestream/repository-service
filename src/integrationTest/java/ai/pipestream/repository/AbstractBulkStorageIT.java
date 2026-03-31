package ai.pipestream.repository;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.http.TestHTTPResource;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for bulk document storage timing tests.
 * <p>
 * Loads 102 pre-parsed PipeDoc protobuf files, stores them via gRPC,
 * retrieves each one, and verifies SHA-256 checksum integrity.
 * Reports timing for store and retrieve operations.
 * <p>
 * Subclasses set the storage mode via {@code @TestProfile} — one for
 * s3-only, one for redis-buffered — giving an apples-to-apples comparison.
 * <p>
 * Results are written to temp files so the comparison summary test can
 * read both sets of timing data.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
abstract class AbstractBulkStorageIT {

    private static final Logger LOG = Logger.getLogger(AbstractBulkStorageIT.class);
    static final String VALID_ACCOUNT = "valid-account";
    static final String DATASOURCE = "ds-bulk-test";
    static final String CONNECTOR = "conn-bulk-test";
    // Sample docs loaded from ai.pipestream:parser-pipedoc-parsed jar on the classpath
    @TempDir
    static Path TIMING_DIR;

    @TestHTTPResource
    URL url;

    private ManagedChannel channel;
    PipeDocServiceGrpc.PipeDocServiceBlockingStub stub;

    /** Stored: docId → nodeId */
    private static final Map<String, String> storedDocs = new LinkedHashMap<>();
    /** Original bytes checksum: docId → sha256 */
    private static final Map<String, String> originalChecksums = new LinkedHashMap<>();
    /** Timing in nanos */
    private static final List<Long> storeTimesNs = new ArrayList<>();
    private static final List<Long> retrieveTimesNs = new ArrayList<>();

    /** Subclass returns the mode label for logging and file output. */
    abstract String modeName();

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

    @BeforeAll
    static void resetState() {
        storedDocs.clear();
        originalChecksums.clear();
        storeTimesNs.clear();
        retrieveTimesNs.clear();
    }

    @Test
    @Order(1)
    void bulkStore() throws IOException {
        List<byte[]> sampleDocs = loadSampleDocs();
        assertThat(sampleDocs)
                .as("Should find sample PipeDoc protobuf files on classpath (parser-pipedoc-parsed jar)")
                .hasSizeGreaterThanOrEqualTo(50);

        LOG.infof("=== [%s] Bulk Store: %d documents ===", modeName(), sampleDocs.size());
        int stored = 0;

        int fileIdx = 0;
        for (byte[] rawBytes : sampleDocs) {
            fileIdx++;
            PipeDoc originalDoc;
            try {
                originalDoc = PipeDoc.parseFrom(rawBytes);
            } catch (Exception e) {
                continue; // skip non-PipeDoc files
            }

            String docId = "bulk-" + modeName() + "-" + fileIdx + "-" + System.nanoTime();
            PipeDoc doc = originalDoc.toBuilder()
                    .setDocId(docId)
                    .setOwnership(OwnershipContext.newBuilder()
                            .setAccountId(VALID_ACCOUNT)
                            .setDatasourceId(DATASOURCE)
                            .setConnectorId(CONNECTOR)
                            .build())
                    .build();

            originalChecksums.put(docId, sha256(doc.toByteArray()));

            long t0 = System.nanoTime();
            SavePipeDocResponse resp = stub.savePipeDoc(
                    SavePipeDocRequest.newBuilder()
                            .setPipedoc(doc)
                            .setDrive("default")
                            .setUseDatasourceId(true)
                            .build());
            long elapsed = System.nanoTime() - t0;

            storeTimesNs.add(elapsed);
            storedDocs.put(docId, resp.getNodeId());
            stored++;
        }

        LongSummaryStatistics stats = storeTimesNs.stream().mapToLong(Long::longValue).summaryStatistics();
        LOG.infof("[%s] Store: %d docs, min=%.1fms, avg=%.1fms, max=%.1fms, total=%.1fs",
                modeName(), stored,
                stats.getMin() / 1e6, stats.getAverage() / 1e6,
                stats.getMax() / 1e6, stats.getSum() / 1e9);

        assertThat(stored).as("All documents should store successfully").isEqualTo(sampleDocs.size());
    }

    @Test
    @Order(2)
    void bulkRetrieve_checksumMatch() {
        assertThat(storedDocs).as("storedDocs populated by prior test").isNotEmpty();

        LOG.infof("=== [%s] Bulk Retrieve: %d documents ===", modeName(), storedDocs.size());
        int matched = 0;
        int failed = 0;

        for (Map.Entry<String, String> entry : storedDocs.entrySet()) {
            String docId = entry.getKey();
            String nodeId = entry.getValue();

            long t0 = System.nanoTime();
            GetPipeDocResponse resp = stub.getPipeDoc(
                    GetPipeDocRequest.newBuilder().setNodeId(nodeId).build());
            long elapsed = System.nanoTime() - t0;
            retrieveTimesNs.add(elapsed);

            assertThat(resp.hasPipedoc())
                    .as("Document should be retrievable: docId=%s, nodeId=%s", docId, nodeId)
                    .isTrue();
            assertThat(resp.getPipedoc().getDocId())
                    .as("Retrieved docId should match for nodeId=%s", nodeId)
                    .isEqualTo(docId);

            String checksum = sha256(resp.getPipedoc().toByteArray());
            if (checksum.equals(originalChecksums.get(docId))) {
                matched++;
            } else {
                LOG.errorf("CHECKSUM MISMATCH: docId=%s expected=%s got=%s",
                        docId, originalChecksums.get(docId), checksum);
                failed++;
            }
        }

        LongSummaryStatistics stats = retrieveTimesNs.stream().mapToLong(Long::longValue).summaryStatistics();
        LOG.infof("[%s] Retrieve: %d docs, min=%.1fms, avg=%.1fms, max=%.1fms, total=%.1fs",
                modeName(), storedDocs.size(),
                stats.getMin() / 1e6, stats.getAverage() / 1e6,
                stats.getMax() / 1e6, stats.getSum() / 1e9);

        assertThat(failed).as("No checksum mismatches").isZero();
        assertThat(matched).as("All documents matched").isEqualTo(storedDocs.size());
    }

    @Test
    @Order(3)
    void writeTimingResults() throws IOException {
        if (storeTimesNs.isEmpty()) return;

        Path outFile = TIMING_DIR.resolve(modeName() + ".properties");

        LongSummaryStatistics store = storeTimesNs.stream().mapToLong(Long::longValue).summaryStatistics();
        LongSummaryStatistics retrieve = retrieveTimesNs.stream().mapToLong(Long::longValue).summaryStatistics();

        String content = String.join("\n",
                "mode=" + modeName(),
                "count=" + storedDocs.size(),
                "store.min.ms=" + String.format("%.2f", store.getMin() / 1e6),
                "store.avg.ms=" + String.format("%.2f", store.getAverage() / 1e6),
                "store.max.ms=" + String.format("%.2f", store.getMax() / 1e6),
                "store.total.s=" + String.format("%.2f", store.getSum() / 1e9),
                "retrieve.min.ms=" + String.format("%.2f", retrieve.getMin() / 1e6),
                "retrieve.avg.ms=" + String.format("%.2f", retrieve.getAverage() / 1e6),
                "retrieve.max.ms=" + String.format("%.2f", retrieve.getMax() / 1e6),
                "retrieve.total.s=" + String.format("%.2f", retrieve.getSum() / 1e9),
                ""
        );
        Files.writeString(outFile, content);
        LOG.infof("[%s] Timing results written to %s", modeName(), outFile);
    }

    /**
     * Loads .pb files from the parser-pipedoc-parsed jar on the classpath.
     * Enumerates known filenames (parsed_document_001.pb through parsed_document_200.pb)
     * since walking a jar filesystem is fragile across different classloader contexts.
     */
    List<byte[]> loadSampleDocs() {
        List<byte[]> docs = new ArrayList<>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        for (int i = 1; i <= 200; i++) {
            String name = String.format("parsed_document_%03d.pb", i);
            try (InputStream is = cl.getResourceAsStream(name)) {
                if (is != null) {
                    docs.add(is.readAllBytes());
                }
            } catch (IOException e) {
                LOG.warnf("Failed to read classpath resource %s: %s", name, e.getMessage());
            }
        }

        if (docs.isEmpty()) {
            LOG.warn("No parsed_document_*.pb files found on classpath — parser-pipedoc-parsed jar missing?");
        } else {
            LOG.infof("Loaded %d sample PipeDoc files from classpath", docs.size());
        }
        return docs;
    }

    static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    }
}
