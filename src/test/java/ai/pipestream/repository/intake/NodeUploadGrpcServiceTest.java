package ai.pipestream.repository.intake;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.filesystem.upload.v1.GetUploadedDocumentRequest;
import ai.pipestream.repository.filesystem.upload.v1.GetUploadedDocumentResponse;
import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;
import ai.pipestream.test.support.S3TestResource;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for NodeUploadGrpcService.
 * Uses real PostgreSQL (via Dev Services), LocalStack S3, and WireMock for Account Service.
 */
@QuarkusTest
@QuarkusTestResource(S3TestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class NodeUploadGrpcServiceTest {

    @GrpcClient("repository-service")
    NodeUploadServiceGrpc.NodeUploadServiceBlockingStub uploadService;

    private PipeDoc createTestDoc(String docId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("valid-account") // Matches stub in AccountManagerMock
                        .setDatasourceId("test-datasource")
                        .setConnectorId("test-connector")
                        .build())
                .build();
    }

    @Test
    void uploadPipeDocStoresAndReturnsIdAndKey() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = createTestDoc(docId);

        UploadFilesystemPipeDocResponse response = uploadService.uploadFilesystemPipeDoc(
                UploadFilesystemPipeDocRequest.newBuilder().setDocument(doc).build());

        assertTrue(response.getSuccess());
        assertEquals(docId, response.getDocumentId());
        assertNotNull(response.getS3Key());
        assertFalse(response.getS3Key().isBlank());
    }

    @Test
    void getDocumentReturnsStoredDoc() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = createTestDoc(docId);
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

    @Test
    void uploadPipeDocWithInvalidAccountFails() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("nonexistent") // Matches NOT_FOUND stub in AccountManagerMock
                        .setDatasourceId("test-datasource")
                        .setConnectorId("test-connector")
                        .build())
                .build();

        assertThrows(StatusRuntimeException.class, () ->
                uploadService.uploadFilesystemPipeDoc(
                        UploadFilesystemPipeDocRequest.newBuilder().setDocument(doc).build()));
    }
}
