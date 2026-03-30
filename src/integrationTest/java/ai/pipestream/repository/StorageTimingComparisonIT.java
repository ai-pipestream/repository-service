package ai.pipestream.repository;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads timing results written by {@link BulkStorageS3OnlyIT} and
 * {@link BulkStorageRedisBufferedIT} and prints a side-by-side comparison.
 * <p>
 * This test has no profile-specific requirements; it just reads temp files.
 * It runs last (alphabetical ordering within the same Gradle task).
 */
@QuarkusIntegrationTest
@TestProfile(RepositoryIntegrationTestProfile.class)
public class StorageTimingComparisonIT {

    private static final Logger LOG = Logger.getLogger(StorageTimingComparisonIT.class);
    private static final Path TIMING_DIR = AbstractBulkStorageIT.TIMING_DIR;

    @Test
    void compareStorageModes() throws IOException {
        Path s3File = TIMING_DIR.resolve("s3-only.properties");
        Path redisFile = TIMING_DIR.resolve("redis-buffered.properties");

        if (!Files.exists(s3File) || !Files.exists(redisFile)) {
            LOG.warnf("Timing files not found (s3=%s, redis=%s). " +
                            "Run BulkStorageS3OnlyIT and BulkStorageRedisBufferedIT first.",
                    Files.exists(s3File), Files.exists(redisFile));
            return;
        }

        Properties s3 = loadProps(s3File);
        Properties redis = loadProps(redisFile);

        String s3StoreAvg = s3.getProperty("store.avg.ms", "?");
        String redisStoreAvg = redis.getProperty("store.avg.ms", "?");
        String s3RetrieveAvg = s3.getProperty("retrieve.avg.ms", "?");
        String redisRetrieveAvg = redis.getProperty("retrieve.avg.ms", "?");

        double storeSpeedup = Double.parseDouble(s3StoreAvg) / Double.parseDouble(redisStoreAvg);
        double retrieveSpeedup = Double.parseDouble(s3RetrieveAvg) / Double.parseDouble(redisRetrieveAvg);

        LOG.infof("╔════════════════════════════════════════════════════════════════════╗");
        LOG.infof("║              STORAGE MODE TIMING COMPARISON                       ║");
        LOG.infof("╠════════════════════════════════════════════════════════════════════╣");
        LOG.infof("║                          S3-Only       Redis-Buffered   Speedup   ║");
        LOG.infof("╠════════════════════════════════════════════════════════════════════╣");
        LOG.infof("║  Documents            %6s            %6s                     ║",
                s3.getProperty("count", "?"), redis.getProperty("count", "?"));
        LOG.infof("╠════════════════════════════════════════════════════════════════════╣");
        LOG.infof("║  STORE                                                            ║");
        LOG.infof("║    Min (ms)       %10s        %10s                     ║",
                s3.getProperty("store.min.ms"), redis.getProperty("store.min.ms"));
        LOG.infof("║    Avg (ms)       %10s        %10s          %5.1fx      ║",
                s3StoreAvg, redisStoreAvg, storeSpeedup);
        LOG.infof("║    Max (ms)       %10s        %10s                     ║",
                s3.getProperty("store.max.ms"), redis.getProperty("store.max.ms"));
        LOG.infof("║    Total (s)      %10s        %10s                     ║",
                s3.getProperty("store.total.s"), redis.getProperty("store.total.s"));
        LOG.infof("╠════════════════════════════════════════════════════════════════════╣");
        LOG.infof("║  RETRIEVE                                                         ║");
        LOG.infof("║    Min (ms)       %10s        %10s                     ║",
                s3.getProperty("retrieve.min.ms"), redis.getProperty("retrieve.min.ms"));
        LOG.infof("║    Avg (ms)       %10s        %10s          %5.1fx      ║",
                s3RetrieveAvg, redisRetrieveAvg, retrieveSpeedup);
        LOG.infof("║    Max (ms)       %10s        %10s                     ║",
                s3.getProperty("retrieve.max.ms"), redis.getProperty("retrieve.max.ms"));
        LOG.infof("║    Total (s)      %10s        %10s                     ║",
                s3.getProperty("retrieve.total.s"), redis.getProperty("retrieve.total.s"));
        LOG.infof("╚════════════════════════════════════════════════════════════════════╝");

        // Verify Redis is at least as fast as S3 for retrieves (allow some variance)
        assertThat(Double.parseDouble(redisRetrieveAvg))
                .as("Redis-buffered retrieve avg should not be significantly slower than S3-only")
                .isLessThan(Double.parseDouble(s3RetrieveAvg) * 2.0);
    }

    private Properties loadProps(Path file) throws IOException {
        Properties props = new Properties();
        props.load(Files.newBufferedReader(file));
        return props;
    }
}
