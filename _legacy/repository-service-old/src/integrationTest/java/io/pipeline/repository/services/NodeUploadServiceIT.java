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

import java.time.OffsetDateTime;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
public class NodeUploadServiceIT {

    @GrpcClient("node-upload-service")
    MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub client;

    @Inject
    S3Service s3Service;

    @Inject
    DriveRepository driveRepository;

    @Inject
    NodeRepository nodeRepository;

    private static final String TEST_DRIVE_NAME = "test-drive-it";
    private Drive testDrive;

    @BeforeEach
    @Transactional
    void setUp() {
        // Given: a valid Drive entity exists in the database for our upload.
        testDrive = new Drive();
        testDrive.name = TEST_DRIVE_NAME;
        testDrive.bucketName = "test-bucket"; // Using the default MinIO bucket from config
        testDrive.createdAt = OffsetDateTime.now();
        driveRepository.persist(testDrive);

        // And: the corresponding S3 bucket exists.
        s3Service.createBucketIfNotExists(testDrive.bucketName, null);
    }

    @AfterEach
    @Transactional
    void tearDown() {
        // Clean up database entities created during the test.
        nodeRepository.delete("driveId", testDrive.id);
        driveRepository.delete(testDrive);
    }

    @Test
    void givenLargePayload_whenUploadIsStreamed_thenFileIsStoredCorrectly() {
        // Given: An upload is initiated.
        int fileSize = 5 * 1024 * 1024; // 5 MB
        InitiateUploadRequest initiateRequest = InitiateUploadRequest.newBuilder()
                .setDriveName(TEST_DRIVE_NAME)
                .setName("my-streaming-file.txt")
                .setMimeType("text/plain")
                .setExpectedSize(fileSize)
                .build();

        InitiateUploadResponse initiateResponse = client.initiateUpload(initiateRequest).await().indefinitely();
        assertThat(initiateResponse, is(notNullValue()));
        String nodeId = initiateResponse.getNodeId();
        String uploadId = initiateResponse.getUploadId();

        // And: A stream of data chunks is prepared.
        byte[] randomData = new byte[fileSize];
        new Random().nextBytes(randomData);
        Multi<UploadChunkRequest> chunkStream = createChunkStream(nodeId, uploadId, fileSize, randomData);

        // When: The chunks are streamed to the server.
        client.uploadChunks(chunkStream).await().indefinitely();

        // Then: The final Node entity in the database is updated correctly.
        Node finalNode = nodeRepository.findByDocumentId(nodeId);
        assertThat(finalNode, is(notNullValue()));
        assertThat(finalNode.size, is((long) fileSize));
        assertThat(finalNode.s3Key, is(nodeId + ".pb"));
        assertThat(finalNode.contentType, is("text/plain"));

        // And: The file content in S3 matches the original data.
        byte[] s3Data = s3Service.retrieveProtobuf(testDrive.bucketName, nodeId);
        assertThat(s3Data, is(equalTo(randomData)));

        // Cleanup the created S3 object for test isolation.
        s3Service.deleteProtobuf(testDrive.bucketName, nodeId);
    }

    private Multi<UploadChunkRequest> createChunkStream(String nodeId, String uploadId, int fileSize, byte[] data) {
        int chunkSize = 64 * 1024; // 64 KB
        return Multi.createFrom().emitter(emitter -> {
            for (int i = 0; i < fileSize; i += chunkSize) {
                int end = Math.min(i + chunkSize, fileSize);
                ByteString chunkData = ByteString.copyFrom(data, i, end - i);
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
    }
}
