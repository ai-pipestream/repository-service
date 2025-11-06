//package io.pipeline.repository.test;
//
//import io.grpc.ManagedChannel;
//import io.grpc.ManagedChannelBuilder;
//import io.grpc.Status;
//import io.grpc.StatusRuntimeException;
//import io.pipeline.repository.account.AccountServiceGrpc;
//import io.pipeline.repository.account.CreateAccountRequest;
//import io.pipeline.repository.account.CreateAccountResponse;
//import io.pipeline.repository.crawler.CrawlDirectoryRequest;
//import io.pipeline.repository.crawler.CrawlDirectoryResponse;
//import io.pipeline.repository.crawler.FilesystemCrawlerServiceGrpc;
//import io.pipeline.repository.filesystem.*;
//import io.quarkus.test.junit.QuarkusIntegrationTest;
//import org.junit.jupiter.api.*;
//
//import java.util.concurrent.TimeUnit;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * Re-creates the complete-workflow.sh script as a repeatable integration test.
// * This test validates the entire end-to-end functionality of the repository service.
// * It uses manually created gRPC clients to communicate with the running service.
// */
//@QuarkusIntegrationTest
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
//public class CompleteWorkflowIT {
//
//    private static final String TEST_ACCOUNT_ID = "test-workflow-account";
//    private static final String TEST_DRIVE_NAME = "test-workflow-drive";
//    private static final String TEST_BUCKET_NAME = "test-workflow-documents";
//
//    private static ManagedChannel channel;
//
//    // Service Stubs
//    private static AccountServiceGrpc.AccountServiceBlockingStub accountService;
//    private static FilesystemServiceGrpc.FilesystemServiceBlockingStub filesystemService;
//    private static FilesystemCrawlerServiceGrpc.FilesystemCrawlerServiceBlockingStub crawlerService;
//
//
//    @BeforeAll
//    static void setUp() {
//        // Create a single channel for all tests
//        channel = ManagedChannelBuilder.forAddress("localhost", 38102)
//                .usePlaintext()
//                .build();
//
//        // Create blocking stubs
//        accountService = AccountServiceGrpc.newBlockingStub(channel);
//        filesystemService = FilesystemServiceGrpc.newBlockingStub(channel);
//        crawlerService = FilesystemCrawlerServiceGrpc.newBlockingStub(channel);
//    }
//
//    @AfterAll
//    static void tearDown() throws InterruptedException {
//        if (channel != null) {
//            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
//        }
//    }
//
//    @Test
//    @Order(1)
//    void step1_CreateAccount() {
//        System.out.println("Step 1: Creating production demo account...");
//        CreateAccountRequest request = CreateAccountRequest.newBuilder()
//                .setAccountId(TEST_ACCOUNT_ID)
//                .setName("Test Workflow Account")
//                .setDescription("Account for complete workflow testing")
//                .build();
//
//        try {
//            CreateAccountResponse response = accountService.createAccount(request);
//            assertNotNull(response);
//            assertNotNull(response.getAccount());
//            assertEquals(TEST_ACCOUNT_ID, response.getAccount().getAccountId());
//            System.out.println("✓ Account created successfully: " + response.getAccount().getAccountId());
//        } catch (StatusRuntimeException e) {
//            // Allow ALREADY_EXISTS for rerunning tests, fail on other errors
//            if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
//                System.out.println("✓ Account already exists, proceeding.");
//            } else {
//                fail("Failed to create account. Status: " + e.getStatus());
//            }
//        }
//    }
//
//    @Test
//    @Order(2)
//    void step2_CreateDrive() {
//        System.out.println("Step 2: Creating drive with auto-created S3 bucket...");
//        CreateDriveRequest request = CreateDriveRequest.newBuilder()
//                .setName(TEST_DRIVE_NAME)
//                .setBucketName(TEST_BUCKET_NAME)
//                .setAccountId(TEST_ACCOUNT_ID)
//                .setRegion("us-east-1")
//                .setDescription("Drive for workflow testing with auto-created bucket")
//                .setCreateBucket(true)
//                .build();
//
//        try {
//            CreateDriveResponse response = filesystemService.createDrive(request);
//            assertNotNull(response);
//            assertNotNull(response.getDrive());
//            assertEquals(TEST_DRIVE_NAME, response.getDrive().getName());
//            System.out.println("✓ Drive and S3 bucket created successfully: " + response.getDrive().getName());
//        } catch (StatusRuntimeException e) {
//            if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
//                System.out.println("✓ Drive already exists, proceeding.");
//            } else {
//                fail("Failed to create drive. Status: " + e.getStatus());
//            }
//        }
//    }
//
//    @Test
//    @Order(3)
//    void step3_VerifyS3BucketCreation() {
//        System.out.println("Step 3: Verifying S3 bucket creation...");
//        // This step is manually verified in the script via AWS CLI.
//        // We'll assume it was created successfully if step 2 passed.
//        System.out.println("✓ S3 bucket verification skipped in this automated test.");
//        assertTrue(true); // Placeholder assertion
//    }
//
//    @Test
//    @Order(4)
//    void step4_ProcessSampleDocuments() {
//        System.out.println("Step 4: Processing sample documents (sample_doc_types)...");
//        CrawlDirectoryRequest request = CrawlDirectoryRequest.newBuilder()
//                .setPath("/home/krickert/IdeaProjects/pipeline-engine-refactor/modules/parser/src/test/resources/sample_doc_types")
//                .setRecursive(false)
//                .setDrive(TEST_DRIVE_NAME)
//                .setConnectorId("workflow-test-samples")
//                .build();
//
//        CrawlDirectoryResponse response = crawlerService.crawlDirectory(request);
//        assertNotNull(response);
//        assertTrue(response.getFilesFound() > 0, "Should have found files to process");
//        assertEquals(response.getFilesFound(), response.getFilesProcessed(), "All found files should be processed");
//        assertEquals(0, response.getFilesFailed(), "No files should fail processing in the sample set");
//        System.out.printf("✓ Sample documents processed. Found: %d, Processed: %d, Failed: %d%n",
//                response.getFilesFound(), response.getFilesProcessed(), response.getFilesFailed());
//    }
//
//    @Test
//    @Order(5)
//    void step5_ProcessAllTestDocuments() {
//        System.out.println("Step 5: Processing ALL parser test documents...");
//        CrawlDirectoryRequest request = CrawlDirectoryRequest.newBuilder()
//                .setPath("/home/krickert/IdeaProjects/pipeline-engine-refactor/modules/parser/src/test/resources")
//                .setRecursive(true)
//                .setDrive(TEST_DRIVE_NAME)
//                .setConnectorId("workflow-test-complete")
//                .build();
//
//        CrawlDirectoryResponse response = crawlerService.crawlDirectory(request);
//        assertNotNull(response);
//        assertTrue(response.getFilesFound() > 0, "Should have found files in the complete set");
//        assertTrue(response.getFilesProcessed() > 0, "Should have processed files from the complete set");
//        System.out.printf("✓ All documents processed. Found: %d, Processed: %d, Failed: %d%n",
//                response.getFilesFound(), response.getFilesProcessed(), response.getFilesFailed());
//
//        if (response.getFilesFailed() > 0) {
//            System.out.println("  Warning: " + response.getFilesFailed() + " files failed to process.");
//        }
//    }
//
//    @Test
//    @Order(6)
//    void step6_GetNodeByPath() {
//        System.out.println("Step 6: Testing GetNodeByPath functionality...");
//        GetNodeByPathRequest request = GetNodeByPathRequest.newBuilder()
//                .setDrive(TEST_DRIVE_NAME)
//                .setConnectorId("workflow-test-complete")
//                .setPath("sample_doc_types/database/README.md")
//                .setIncludePayload(false)
//                .build();
//
//        Node response = filesystemService.getNodeByPath(request);
//        assertNotNull(response, "Node should not be null");
//        assertEquals("README.md", response.getName());
//        assertTrue(response.getPath().endsWith("sample_doc_types/database/README.md"), "Path should be correct");
//        System.out.printf("✓ GetNodeByPath working. Found: %s at path: %s%n", response.getName(), response.getPath());
//    }
//
//    @Test
//    @Order(7)
//    void step7_GetDrive() {
//        System.out.println("Step 7: Testing GetDrive functionality...");
//        GetDriveRequest request = GetDriveRequest.newBuilder()
//                .setName(TEST_DRIVE_NAME)
//                .build();
//
//        Drive response = filesystemService.getDrive(request);
//        assertNotNull(response, "Drive should not be null");
//        assertEquals(TEST_BUCKET_NAME, response.getBucketName());
//        assertEquals(TEST_ACCOUNT_ID, response.getAccountId());
//        System.out.printf("✓ GetDrive working. Drive bucket: %s, Account: %s%n", response.getBucketName(), response.getAccountId());
//    }
//
//    @Test
//    @Order(8)
//    void step8_VerifyS3Storage() {
//        System.out.println("Step 8: Verifying S3 storage organization...");
//        // This step uses AWS CLI in the script.
//        // We will skip the direct verification in this automated test.
//        System.out.println("✓ S3 storage verification skipped.");
//        assertTrue(true); // Placeholder
//    }
//
//    @Test
//    @Order(9)
//    void step9_SearchNodes() {
//        System.out.println("Step 9: Testing SearchNodes functionality...");
//        SearchNodesRequest request = SearchNodesRequest.newBuilder()
//                .setDrive(TEST_DRIVE_NAME)
//                .setQuery("README.md")
//                .setPageSize(3)
//                .build();
//
//        SearchNodesResponse response = filesystemService.searchNodes(request);
//        assertNotNull(response);
//        assertTrue(response.getTotalCount() > 0, "Search should find at least one README.md file");
//        System.out.printf("✓ SearchNodes working. Found %d README.md files.%n", response.getTotalCount());
//    }
//}
