package io.pipeline.repository.managers;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.quarkus.test.junit.QuarkusTest;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EntityTestHelper;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class UploadManagerTest {

    @Inject
    UploadManager uploadManager;

    @Inject
    TestBucketManager bucketManager;

    @Inject
    EntityTestHelper entityHelper;

    private String testBucket;

    @BeforeEach
    @Transactional
    void setUp(TestInfo testInfo) {
        testBucket = bucketManager.setupBucket(testInfo);
        entityHelper.createTestDrive("test-drive", testBucket);
    }

    @Test
    void testUploadProtobufSmall() {
        // Create a small protobuf
        StringValue testValue = StringValue.of("Test protobuf content");
        Any protobufPayload = Any.pack(testValue);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("test", "metadata");

        // Upload
        UploadManager.UploadResult result = uploadManager.uploadProtobuf(
            testBucket, "test-connector", "docs/2024", "test.pb",
            protobufPayload, metadata
        ).await().indefinitely();

        // Verify result
        assertNotNull(result);
        assertNotNull(result.nodeId);
        assertEquals("connectors/test-connector/docs/2024/" + result.nodeId + ".pb", result.s3Key);
        assertTrue(result.size > 0);
        assertFalse(result.wasMultipart); // Should be small upload
    }

    @Test
    void testUploadBytes() {
        byte[] testData = "Test file content".getBytes();
        Map<String, String> metadata = new HashMap<>();

        UploadManager.UploadResult result = uploadManager.uploadBytes(
            testBucket, "file-connector", null, "test.txt",
            testData, "text/plain", metadata
        ).await().indefinitely();

        assertNotNull(result);
        assertTrue(result.s3Key.startsWith("connectors/file-connector/"));
        assertTrue(result.s3Key.endsWith(".pb"));
        assertEquals(testData.length, result.size);
    }

    @Test
    void testInitiateUploadSmall() {
        Map<String, String> metadata = new HashMap<>();

        UploadManager.InitiateUploadResult result = uploadManager.initiateUpload(
            testBucket, "stream-connector", "uploads", "small-file.bin",
            1024, "application/octet-stream", metadata
        ).await().indefinitely();

        assertNotNull(result);
        assertNotNull(result.nodeId);
        assertNotNull(result.uploadId);
        assertFalse(result.willUseMultipart); // Small file
    }

    @Test
    void testInitiateUploadLarge() {
        Map<String, String> metadata = new HashMap<>();

        UploadManager.InitiateUploadResult result = uploadManager.initiateUpload(
            testBucket, "stream-connector", "uploads", "large-file.bin",
            10 * 1024 * 1024, "application/octet-stream", metadata
        ).await().indefinitely();

        assertNotNull(result);
        assertTrue(result.willUseMultipart); // Large file
    }
}