package ai.pipestream.repository.service.upload;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.filesystem.upload.GetDocumentRequest;
import ai.pipestream.repository.filesystem.upload.MutinyNodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.UploadResponse;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.UUID;

@Singleton
@GrpcService
public class NodeUploadServiceImpl extends MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(NodeUploadServiceImpl.class);

    @ConfigProperty(name = "aws.s3.bucket.default", defaultValue = "pipeline-documents")
    String bucketName;

    @Inject
    S3AsyncClient s3Client;

    @Override
    public Uni<UploadResponse> uploadPipeDoc(PipeDoc pipeDoc) {
        // 1. Generate IDs
        String docId = UUID.randomUUID().toString();
        String connectorId = pipeDoc.getSearchMetadata().getMetadataOrDefault("connector_id", "unknown");
        String accountId = pipeDoc.getSearchMetadata().getMetadataOrDefault("account_id", "unknown");
        
        // Generate S3 Key: account/connector/path/docId-filename
        String filename = "unknown.bin";
        String mimeType;
        
        if (pipeDoc.hasBlobBag() && pipeDoc.getBlobBag().hasBlob()) {
             filename = pipeDoc.getBlobBag().getBlob().getFilename();
             mimeType = pipeDoc.getBlobBag().getBlob().getMimeType();
        } else {
            mimeType = "application/octet-stream";
        }

        String path = pipeDoc.getSearchMetadata().getSourcePath();
        if (path == null || path.isEmpty()) {
            path = "root";
        }
        // Sanitize path to avoid double slashes
        if (path.startsWith("/")) path = path.substring(1);
        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);

        String s3Key = String.format("%s/%s/%s/%s-%s", accountId, connectorId, path, docId, filename);

        LOG.debugf("Starting upload for docId: %s to key: %s", docId, s3Key);

        // 2. Upload to S3 (Synchronous wait logic wrapped in Uni)
        // We use Uni.createFrom().completionStage to bridge the CompletableFuture from S3 SDK
        String finalPath = path;
        return Uni.createFrom().completionStage(() -> {
            byte[] bytes = pipeDoc.toByteArray();
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(mimeType)
                    .metadata(pipeDoc.getSearchMetadata().getMetadataMap())
                    .build();

            return s3Client.putObject(putRequest, AsyncRequestBody.fromBytes(bytes))
                    .thenApply(resp -> {
                        LOG.debugf("S3 upload complete. ETag: %s", resp.eTag());
                        return s3Key;
                    });
        })
        .map(uploadedKey -> {
            return UploadResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(docId)
                    .setS3Key(uploadedKey)
                    .setMessage("Upload successful")
                    .build();
        })
        .onFailure().recoverWithItem(t -> {
            LOG.errorf(t, "Upload failed for docId: %s", docId);
            return UploadResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Upload failed: " + t.getMessage())
                    .build();
        });
    }

    @Override
    public Uni<PipeDoc> getDocument(GetDocumentRequest request) {
        return Uni.createFrom().failure(new UnsupportedOperationException("GetDocument not implemented yet"));
    }
}
