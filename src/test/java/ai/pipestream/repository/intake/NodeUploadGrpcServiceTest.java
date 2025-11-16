package ai.pipestream.repository.intake;

import ai.pipestream.repository.filesystem.upload.*;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for NodeUploadGrpcService.
 * Tests the complete gRPC service flow including idempotency and validation.
 */
@QuarkusTest
class NodeUploadGrpcServiceTest {

    @GrpcClient
    NodeUploadServiceGrpc.NodeUploadServiceBlockingStub uploadService;

    @Inject
    InMemoryUploadStateStore stateStore;

    @BeforeEach
    void setUp() {
        stateStore.cleanup();
    }

    @Test
    void testInitiateUpload() {
        InitiateUploadRequest request = InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .setMimeType("application/pdf")
                .build();

        InitiateUploadResponse response = uploadService.initiateUpload(request);

        assertNotNull(response.getNodeId());
        assertNotNull(response.getUploadId());
        assertFalse(response.getNodeId().isEmpty());
        assertFalse(response.getUploadId().isEmpty());
        assertEquals(UploadState.UPLOAD_STATE_UPLOADING, response.getState());
        assertTrue(response.getCreatedAtEpochMs() > 0);
        assertFalse(response.getIsUpdate());
    }

    @Test
    void testInitiateUploadWithClientNodeId() {
        String clientNodeId = UUID.randomUUID().toString();
        InitiateUploadRequest request = InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .setClientNodeId(clientNodeId)
                .build();

        InitiateUploadResponse response = uploadService.initiateUpload(request);

        assertEquals(clientNodeId, response.getNodeId());
        assertNotNull(response.getUploadId());
        assertNotEquals(clientNodeId, response.getUploadId()); // uploadId should be different
    }

    @Test
    void testUploadChunk() {
        // First initiate an upload
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        // Upload a chunk
        byte[] chunkData = new byte[1024];
        for (int i = 0; i < chunkData.length; i++) {
            chunkData[i] = (byte) (i % 256);
        }

        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .setUploadId(initResponse.getUploadId())
                .setChunkNumber(0)
                .setData(ByteString.copyFrom(chunkData))
                .setIsLast(false)
                .build();

        UploadChunkResponse response = uploadService.uploadChunk(chunkRequest);

        assertEquals(initResponse.getNodeId(), response.getNodeId());
        assertEquals(UploadState.UPLOAD_STATE_UPLOADING, response.getState());
        assertEquals(1024, response.getBytesUploaded());
        assertEquals(0, response.getChunkNumber());
    }

    @Test
    void testUploadMultipleChunks() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        // Upload multiple chunks
        for (int i = 0; i < 5; i++) {
            byte[] chunkData = new byte[1000];
            UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                    .setNodeId(initResponse.getNodeId())
                    .setUploadId(initResponse.getUploadId())
                    .setChunkNumber(i)
                    .setData(ByteString.copyFrom(chunkData))
                    .setIsLast(i == 4)
                    .build();

            UploadChunkResponse response = uploadService.uploadChunk(chunkRequest);
            assertEquals((i + 1) * 1000, response.getBytesUploaded());
        }
    }

    @Test
    void testUploadChunkIdempotency() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        byte[] chunkData = new byte[1024];
        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .setUploadId(initResponse.getUploadId())
                .setChunkNumber(0)
                .setData(ByteString.copyFrom(chunkData))
                .setIsLast(false)
                .build();

        // First upload
        UploadChunkResponse firstResponse = uploadService.uploadChunk(chunkRequest);
        assertEquals(1024, firstResponse.getBytesUploaded());

        // Duplicate upload - should be idempotent
        UploadChunkResponse secondResponse = uploadService.uploadChunk(chunkRequest);
        assertEquals(1024, secondResponse.getBytesUploaded()); // Bytes should NOT increase
    }

    @Test
    void testUploadChunkOutOfOrder() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        // Upload chunks out of order
        int[] order = {3, 1, 4, 0, 2};
        for (int chunkNum : order) {
            byte[] chunkData = new byte[1000];
            UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                    .setNodeId(initResponse.getNodeId())
                    .setUploadId(initResponse.getUploadId())
                    .setChunkNumber(chunkNum)
                    .setData(ByteString.copyFrom(chunkData))
                    .build();

            UploadChunkResponse response = uploadService.uploadChunk(chunkRequest);
            assertNotNull(response);
        }

        // Verify final state
        GetUploadStatusResponse status = uploadService.getUploadStatus(
            GetUploadStatusRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .build());
        assertEquals(5000, status.getBytesUploaded());
    }

    @Test
    void testUploadChunkNonExistentUpload() {
        byte[] chunkData = new byte[1024];
        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                .setNodeId(UUID.randomUUID().toString())
                .setUploadId(UUID.randomUUID().toString())
                .setChunkNumber(0)
                .setData(ByteString.copyFrom(chunkData))
                .build();

        assertThrows(StatusRuntimeException.class, () -> {
            uploadService.uploadChunk(chunkRequest);
        });
    }

    @Test
    void testUploadChunkInvalidChunkNumber() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        byte[] chunkData = new byte[1024];
        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .setUploadId(initResponse.getUploadId())
                .setChunkNumber(-1) // Invalid
                .setData(ByteString.copyFrom(chunkData))
                .build();

        assertThrows(StatusRuntimeException.class, () -> {
            uploadService.uploadChunk(chunkRequest);
        });
    }

    @Test
    void testUploadChunkEmptyData() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .setUploadId(initResponse.getUploadId())
                .setChunkNumber(0)
                .setData(ByteString.EMPTY)
                .build();

        assertThrows(StatusRuntimeException.class, () -> {
            uploadService.uploadChunk(chunkRequest);
        });
    }

    @Test
    void testGetUploadStatus() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        // Upload some chunks
        for (int i = 0; i < 3; i++) {
            byte[] chunkData = new byte[1000];
            uploadService.uploadChunk(
                UploadChunkRequest.newBuilder()
                    .setNodeId(initResponse.getNodeId())
                    .setUploadId(initResponse.getUploadId())
                    .setChunkNumber(i)
                    .setData(ByteString.copyFrom(chunkData))
                    .build());
        }

        GetUploadStatusResponse status = uploadService.getUploadStatus(
            GetUploadStatusRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .build());

        assertEquals(initResponse.getNodeId(), status.getNodeId());
        assertEquals(UploadState.UPLOAD_STATE_UPLOADING, status.getState());
        assertEquals(3000, status.getBytesUploaded());
        assertTrue(status.getUpdatedAtEpochMs() > 0);
    }

    @Test
    void testGetUploadStatusNonExistent() {
        GetUploadStatusResponse status = uploadService.getUploadStatus(
            GetUploadStatusRequest.newBuilder()
                .setNodeId(UUID.randomUUID().toString())
                .build());

        // Should return UNKNOWN status
        assertEquals(UploadState.UPLOAD_STATE_UNSPECIFIED, status.getState());
        assertEquals("Upload not found", status.getErrorMessage());
    }

    @Test
    void testCancelUpload() {
        InitiateUploadResponse initResponse = uploadService.initiateUpload(
            InitiateUploadRequest.newBuilder()
                .setDrive("test-drive")
                .setName("test-file.pdf")
                .build());

        CancelUploadResponse cancelResponse = uploadService.cancelUpload(
            CancelUploadRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .build());

        assertTrue(cancelResponse.getSuccess());
        assertEquals("Upload cancelled successfully", cancelResponse.getMessage());

        // Verify it's gone
        GetUploadStatusResponse status = uploadService.getUploadStatus(
            GetUploadStatusRequest.newBuilder()
                .setNodeId(initResponse.getNodeId())
                .build());
        assertEquals(UploadState.UPLOAD_STATE_UNSPECIFIED, status.getState());
    }

    @Test
    void testCancelNonExistentUpload() {
        CancelUploadResponse cancelResponse = uploadService.cancelUpload(
            CancelUploadRequest.newBuilder()
                .setNodeId(UUID.randomUUID().toString())
                .build());

        assertFalse(cancelResponse.getSuccess());
        assertEquals("Upload not found", cancelResponse.getMessage());
    }
}
