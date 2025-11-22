package io.pipeline.repository.services;

import com.google.protobuf.ByteString;
import io.pipeline.repository.data.DriveRepository;
import io.pipeline.repository.data.NodeRepository;
import io.pipeline.repository.filesystem.upload.InitiateUploadRequest;
import io.pipeline.repository.filesystem.upload.InitiateUploadResponse;
import io.pipeline.repository.filesystem.upload.MutinyNodeUploadServiceGrpc;
import io.pipeline.repository.filesystem.upload.UploadChunkRequest;
import io.pipeline.repository.model.Drive;
import io.pipeline.repository.model.Node;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.OffsetDateTime;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
public class NodeUploadServiceImplTest {

    @GrpcClient("node-upload-service")
    MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub client;

    @Inject
    S3Service s3Service;

    @Inject
    DriveRepository driveRepository;

    @Inject
    NodeRepository nodeRepository;

    private static final String TEST_DRIVE_NAME = "test-drive";
    private Drive testDrive;

    @BeforeEach
    @Transactional
    void setUp() {
        // 1. Create a test drive to upload into
        testDrive = new Drive();
        testDrive.name = TEST_DRIVE_NAME;
        testDrive.bucketName = "test-bucket"; // Using the default MinIO bucket from config
        testDrive.createdAt = OffsetDateTime.now();
        driveRepository.persist(testDrive);

        // 2. Ensure the S3 bucket exists
        s3Service.createBucketIfNotExists(testDrive.bucketName, null);
    }

    @AfterEach
    @Transactional
    void tearDown() {
        // Clean up database
        nodeRepository.delete("driveId", testDrive.id);
        driveRepository.delete(testDrive);
    }

    @Test
    void testStreamingUpload() {
        // 1. Initiate the upload
        InitiateUploadRequest initiateRequest = InitiateUploadRequest.newBuilder()
                .setDriveName(TEST_DRIVE_NAME)
                .setName("my-streaming-file.txt")
                .setMimeType("text/plain")
                .setExpectedSize(10 * 1024 * 1024) // 10 MB
                .build();

        InitiateUploadResponse initiateResponse = client.initiateUpload(initiateRequest).await().indefinitely();
        assertThat(initiateResponse).isNotNull();
        String nodeId = initiateResponse.getNodeId();
        String uploadId = initiateResponse.getUploadId();

        // 2. Prepare the data stream
        int fileSize = 10 * 1024 * 1024; // 10 MB
        int chunkSize = 64 * 1024; // 64 KB
        byte[] randomData = new byte[fileSize];
        new Random().nextBytes(randomData);

        Multi<UploadChunkRequest> chunkStream = Multi.createFrom().emitter(emitter -> {
            for (int i = 0; i < fileSize; i += chunkSize) {
                int end = Math.min(i + chunkSize, fileSize);
                ByteString chunkData = ByteString.copyFrom(randomData, i, end - i);
                UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                        .setNodeId(nodeId)
                        .setUploadId(uploadId)
                        .setDriveName(TEST_DRIVE_NAME)
                        .setMimeType("text/plain")
                        .setTotalSize(fileSize)
                        .setData(chunkData)
                        .build();
                emitter.emit(chunkRequest);
            }
            emitter.complete();
        });

        // 3. Perform the streaming upload
        client.uploadChunks(chunkStream).await().indefinitely();

        // 4. Verify the result
        Node finalNode = nodeRepository.findByDocumentId(nodeId);
        assertThat(finalNode).isNotNull();
        assertThat(finalNode.size).isEqualTo(fileSize);
        assertThat(finalNode.s3Key).isEqualTo(nodeId + ".pb");
        assertThat(finalNode.contentType).isEqualTo("text/plain");

        // 5. Verify the file in S3
        byte[] s3Data = s3Service.retrieveProtobuf(testDrive.bucketName, nodeId);
        assertThat(s3Data).isEqualTo(randomData);

        // 6. Clean up S3 object
        s3Service.deleteProtobuf(testDrive.bucketName, nodeId);
    }
}
