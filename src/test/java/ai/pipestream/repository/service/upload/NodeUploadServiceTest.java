package ai.pipestream.repository.service.upload;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.repository.filesystem.upload.UploadResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NodeUploadServiceTest {

    @Mock
    S3AsyncClient s3Client;

    NodeUploadServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new NodeUploadServiceImpl();
        service.s3Client = s3Client;
        service.bucketName = "test-bucket";
    }

    @Test
    void uploadPipeDoc_success() {
        // Arrange
        SearchMetadata metadata = SearchMetadata.newBuilder()
                .putMetadata("connector_id", "conn-1")
                .putMetadata("account_id", "acc-1")
                .setSourcePath("/path/to/file.txt")
                .build();
        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setSearchMetadata(metadata)
                .build();

        // Mock S3
        when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("etag-123").build()));

        // Act
        UploadResponse response = service.uploadPipeDoc(pipeDoc).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertNotNull(response.getDocumentId());
        assertNotNull(response.getS3Key());
        assertTrue(response.getS3Key().contains("acc-1/conn-1/path/to/file.txt"));

        // Verify S3 call
        ArgumentCaptor<PutObjectRequest> s3RequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(s3RequestCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("test-bucket", s3RequestCaptor.getValue().bucket());
        assertEquals(response.getS3Key(), s3RequestCaptor.getValue().key());
    }

    @Test
    void uploadPipeDoc_s3Failure_returnsError() {
        // Arrange
        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.getDefaultInstance())
                .build();

        // Mock S3 failure
        when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("S3 Error")));

        // Act
        UploadResponse response = service.uploadPipeDoc(pipeDoc).await().indefinitely();

        // Assert
        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("S3 Error"));
    }
}
