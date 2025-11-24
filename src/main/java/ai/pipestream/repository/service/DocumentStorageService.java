package ai.pipestream.repository.service;

import ai.pipestream.data.v1.PipeDoc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.util.HashMap;
import java.util.Map;

/**
 * Service for managing document storage operations using S3 Transfer Manager.
 * Handles high-throughput, non-blocking uploads and downloads.
 */
@ApplicationScoped
public class DocumentStorageService {

    private static final Logger LOG = Logger.getLogger(DocumentStorageService.class);
    private static final String PROTOBUF_EXTENSION = ".pb";

    @Inject
    S3TransferManager transferManager;

    @Inject
    software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient;

    /**
     * Uploads a PipeDoc to S3.
     *
     * @param bucketName  The target S3 bucket.
     * @param connectorId The connector ID (namespace).
     * @param pipeDoc     The document to upload.
     * @return A Uni containing the S3 ETag upon success.
     */
    public Uni<String> uploadPipeDoc(String bucketName, String connectorId, PipeDoc pipeDoc) {
        String docId = pipeDoc.getDocId();
        String s3Key = buildS3Key(connectorId, docId);
        byte[] data = pipeDoc.toByteArray();

        LOG.infof("Starting upload for doc: %s to s3://%s/%s (%d bytes)", docId, bucketName, s3Key, data.length);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("doc-id", docId);
        metadata.put("connector-id", connectorId);
        metadata.put("content-type", "application/x-protobuf");

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/x-protobuf")
                .metadata(metadata)
                .build();

        UploadRequest uploadRequest = UploadRequest.builder()
                .putObjectRequest(putObjectRequest)
                .requestBody(AsyncRequestBody.fromBytes(data))
                .addTransferListener(LoggingTransferListener.create())
                .build();

        return Uni.createFrom().completionStage(transferManager.upload(uploadRequest).completionFuture())
                .onItem().transform(CompletedUpload::response)
                .onItem().transform(response -> {
                    LOG.infof("Upload complete for %s. ETag: %s", s3Key, response.eTag());
                    return response.eTag();
                })
                .onFailure().invoke(th -> LOG.errorf(th, "Failed to upload document %s", docId));
    }

    /**
     * Downloads a PipeDoc from S3.
     *
     * @param bucketName  The source S3 bucket.
     * @param connectorId The connector ID.
     * @param docId       The document ID.
     * @return A Uni containing the PipeDoc.
     */
    public Uni<PipeDoc> downloadPipeDoc(String bucketName, String connectorId, String docId) {
        String s3Key = buildS3Key(connectorId, docId);
        LOG.infof("Downloading doc: %s from s3://%s/%s", docId, bucketName, s3Key);

        return Uni.createFrom().completionStage(
                s3AsyncClient.getObject(
                        software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(s3Key)
                                .build(),
                        software.amazon.awssdk.core.async.AsyncResponseTransformer.toBytes()))
                .onItem().transform(response -> {
                    try {
                        return PipeDoc.parseFrom(response.asByteArray());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse PipeDoc protobuf", e);
                    }
                })
                .onFailure().invoke(th -> LOG.errorf(th, "Failed to download document %s", docId));
    }

    private String buildS3Key(String connectorId, String docId) {
        // Standard path: connectors/{connectorId}/{docId}.pb
        return String.format("connectors/%s/%s%s", connectorId, docId, PROTOBUF_EXTENSION);
    }
}