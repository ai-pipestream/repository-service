package ai.pipestream.repository.intake;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentRequest;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentResponse;
import ai.pipestream.repository.v1.filesystem.upload.NodeUploadServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocResponse;
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
 */
@QuarkusTest
class NodeUploadGrpcServiceTest {

    @GrpcClient
    NodeUploadServiceGrpc.NodeUploadServiceBlockingStub uploadService;

    @Inject
    DocumentStorageService storageService;

    @BeforeEach
    void setUp() {
        storageService.clear();
    }

    @Test
    void uploadPipeDocStoresAndReturnsIdAndKey() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = PipeDoc.newBuilder().setDocId(docId).build();

        UploadFilesystemPipeDocResponse response = uploadService.uploadFilesystemPipeDoc(
                UploadFilesystemPipeDocRequest.newBuilder().setDocument(doc).build());

        assertTrue(response.getSuccess());
        assertEquals(docId, response.getDocumentId());
        assertFalse(response.getS3Key().isBlank());
    }

    @Test
    void getDocumentReturnsStoredDoc() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = PipeDoc.newBuilder().setDocId(docId).build();
        uploadService.uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest.newBuilder().setDocument(doc).build());

        GetUploadedDocumentResponse response = uploadService.getUploadedDocument(
                GetUploadedDocumentRequest.newBuilder().setDocumentId(docId).build());

        assertNotNull(response);
        assertTrue(response.hasDocument());
        assertEquals(docId, response.getDocument().getDocId());
    }

    @Test
    void getDocumentNotFoundReturnsNotFound() {
        assertThrows(StatusRuntimeException.class, () ->
                uploadService.getUploadedDocument(GetUploadedDocumentRequest.newBuilder()
                        .setDocumentId(UUID.randomUUID().toString())
                        .build()));
    }
}
