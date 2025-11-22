package io.pipeline.repository.services;

import io.quarkus.test.junit.QuarkusTest;
import io.pipeline.repository.crawler.*;
import io.pipeline.repository.test.TestBucketManager;
import io.pipeline.repository.test.EntityTestHelper;
import io.pipeline.repository.entity.Drive;
import io.pipeline.repository.entity.Node;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class FilesystemCrawlerIntegrationTest {

    @Inject
    FilesystemCrawlerServiceImpl crawlerService;

    @Inject
    TestBucketManager bucketManager;

    @Inject
    EntityTestHelper entityHelper;
    
    @Inject
    Drive.DriveService driveService;

    private String testBucket;
    private Drive testDrive;
    private Path testDirectory;

    @BeforeEach
    @Transactional
    void setUp(TestInfo testInfo) throws IOException {
        testBucket = bucketManager.setupBucket(testInfo);
        testDrive = entityHelper.createTestDrive("test-drive", testBucket);

        // Create temporary test directory with various file types
        testDirectory = Files.createTempDirectory("crawler-test");

        // Create test files
        Files.write(testDirectory.resolve("test.txt"), "Simple text content".getBytes());
        Files.write(testDirectory.resolve("test.json"), "{\"key\": \"value\", \"number\": 123}".getBytes());
        Files.write(testDirectory.resolve("test.xml"), "<root><item>data</item></root>".getBytes());

        // Create a larger file for size testing
        StringBuilder largeContent = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeContent.append("Line ").append(i).append(": This is test content for a larger file.\n");
        }
        Files.write(testDirectory.resolve("large.txt"), largeContent.toString().getBytes());
    }

    @Test
    void testCrawlDirectory() {
        CrawlDirectoryRequest request = CrawlDirectoryRequest.newBuilder()
            .setPath(testDirectory.toString())
            .setRecursive(false)
            .setDrive("test-drive")
            .setConnectorId("test-crawler")
            .build();

        CrawlDirectoryResponse response = crawlerService.crawlDirectory(request)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        // Verify response
        assertNotNull(response);
        assertEquals(4, response.getFilesFound());
        assertEquals(4, response.getFilesProcessed());
        assertEquals(0, response.getFilesFailed());
        assertEquals(4, response.getProcessedFilesList().size());
        assertEquals(0, response.getFailedFilesList().size());

        // Verify files were saved to database using proper query
        List<Node> savedNodes = Node.list("driveId", testDrive.id);
        assertEquals(4, savedNodes.size());

        // Verify connector path structure
        for (Node node : savedNodes) {
            assertNotNull(node.s3Key);
            assertTrue(node.s3Key.startsWith("connectors/test-crawler/"));
            assertTrue(node.s3Key.endsWith(".pb"));
            assertNotNull(node.payloadType);
            assertEquals("type.googleapis.com/io.pipeline.data.v1.PipeDoc", node.payloadType);
        }
    }

    @Test
    void testCrawlDirectoryWithPath() {
        // Create subdirectory
        Path subDir = testDirectory.resolve("subdir");
        try {
            Files.createDirectories(subDir);
            Files.write(subDir.resolve("nested.txt"), "Nested file content".getBytes());
        } catch (IOException e) {
            fail("Failed to create test subdirectory");
        }

        CrawlDirectoryRequest request = CrawlDirectoryRequest.newBuilder()
            .setPath(testDirectory.toString())
            .setRecursive(true)
            .setDrive("test-drive")
            .setConnectorId("test-crawler-recursive")
            .build();

        CrawlDirectoryResponse response = crawlerService.crawlDirectory(request)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        // Should find original 4 files + 1 nested file
        assertEquals(5, response.getFilesFound());
        assertEquals(5, response.getFilesProcessed());
        assertEquals(0, response.getFilesFailed());
    }

    @Test
    void testValidation() {
        // Test missing path
        CrawlDirectoryRequest invalidRequest = CrawlDirectoryRequest.newBuilder()
            .setRecursive(false)
            .setDrive("test-drive")
            .setConnectorId("test-crawler")
            .build();

        crawlerService.crawlDirectory(invalidRequest)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitFailure();

        // Test missing drive
        CrawlDirectoryRequest noDriveRequest = CrawlDirectoryRequest.newBuilder()
            .setPath(testDirectory.toString())
            .setRecursive(false)
            .setConnectorId("test-crawler")
            .build();

        crawlerService.crawlDirectory(noDriveRequest)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitFailure();

        // Test missing connector ID
        CrawlDirectoryRequest noConnectorRequest = CrawlDirectoryRequest.newBuilder()
            .setPath(testDirectory.toString())
            .setRecursive(false)
            .setDrive("test-drive")
            .build();

        crawlerService.crawlDirectory(noConnectorRequest)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitFailure();
    }

    @Test
    void testFileTypeHandling() {
        // Test with different file types to verify protobuf creation
        CrawlDirectoryRequest request = CrawlDirectoryRequest.newBuilder()
            .setPath(testDirectory.toString())
            .setRecursive(false)
            .setDrive("test-drive")
            .setConnectorId("test-file-types")
            .build();

        CrawlDirectoryResponse response = crawlerService.crawlDirectory(request)
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        // All text-based files should succeed
        assertEquals(4, response.getFilesProcessed());

        // Verify metadata in database
        List<Node> nodes = Node.list("driveId", testDrive.id);
        for (Node node : nodes) {
            // Should have proper content type detection
            assertNotNull(node.contentType);
            assertTrue(node.contentType.equals("application/x-protobuf"));

            // Should have protobuf type
            assertEquals("type.googleapis.com/io.pipeline.data.v1.PipeDoc", node.payloadType);

            // Should have proper S3 key
            assertTrue(node.s3Key.contains("test-file-types"));
        }
    }
}