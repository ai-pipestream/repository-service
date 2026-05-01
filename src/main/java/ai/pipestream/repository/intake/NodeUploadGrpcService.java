package ai.pipestream.repository.intake;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.config.FeatureFlags;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.filesystem.upload.v1.GetUploadedDocumentRequest;
import ai.pipestream.repository.filesystem.upload.v1.GetUploadedDocumentResponse;
import ai.pipestream.repository.filesystem.upload.v1.MutinyNodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.service.DocumentStorageService.AccountValidationException;
import io.grpc.Status;
import io.micrometer.core.instrument.Timer;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Phase 1 gRPC service implementation for NodeUploadService.
 * Reactive implementation.
 *
 * This is aligned to the current source-of-record repo protos (unary RPCs only):
 * - UploadPipeDoc
 * - GetDocument
 */
@GrpcService
public class NodeUploadGrpcService extends MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(NodeUploadGrpcService.class);

    @Inject
    FeatureFlags featureFlags;

    @Inject
    IntakeMetrics metrics;

    @Inject
    DocumentStorageService storageService;

    @Override
    @WithTransaction
    public Uni<UploadFilesystemPipeDocResponse> uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request) {
        Timer.Sample timerSample = metrics.startInitiateUploadTimer();

        if (!featureFlags.phase1().enabled()) {
            return Uni.createFrom().failure(Status.UNAVAILABLE
                    .withDescription("Phase 1 intake service is not enabled")
                    .asRuntimeException());
        }

        if (request == null || !request.hasDocument()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("document is required")
                    .asRuntimeException());
        }

        PipeDoc document = request.getDocument();

        return storageService.store(document)
                .map(stored -> {
                    metrics.recordUploadInitiated();
                    // Return the doc_id (not node_id) as document_id in the response
                    return UploadFilesystemPipeDocResponse.newBuilder()
                            .setSuccess(true)
                            .setDocumentId(document.getDocId())
                            .setS3Key(stored.s3Key())
                            .setMessage("stored")
                            .build();
                })
                .onFailure(AccountValidationException.class).transform(e -> {
                    LOG.warnf("Account validation failed: %s", e.getMessage());
                    return Status.PERMISSION_DENIED
                            .withDescription(e.getMessage())
                            .asRuntimeException();
                })
                .onFailure(IllegalArgumentException.class).transform(e -> {
                    LOG.warnf("Invalid PipeDoc upload request: %s", e.getMessage());
                    return Status.INVALID_ARGUMENT
                            .withDescription(e.getMessage())
                            .asRuntimeException();
                })
                .onFailure().invoke(e -> LOG.error("Failed to upload PipeDoc", e))
                .eventually(() -> metrics.stopInitiateUploadTimer(timerSample));
    }

    @Override
    @WithSession
    public Uni<GetUploadedDocumentResponse> getUploadedDocument(GetUploadedDocumentRequest request) {
        if (request == null || request.getDocumentId() == null || request.getDocumentId().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("document_id is required")
                    .asRuntimeException());
        }

        String documentId = request.getDocumentId();

        // For initial intake lookups (docId only), query by docId alone and assume it's the initial intake state.
        // This works because initial intake creates only one entry per docId with graph_address_id = datasource_id.
        // Note: If multiple entries exist for the same docId (different graph_address_id values),
        // this will return the first one found. For more precise lookups, use GetPipeDocByReference.
        return PipeDocRecord.<PipeDocRecord>find("docId", documentId).firstResult()
                .flatMap(record -> {
                    if (record == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                                .withDescription("Document not found: " + documentId)
                                .asRuntimeException());
                    }
                    
                    // Fetch the PipeDoc from S3 using the record's nodeId
                    return storageService.get(record.nodeId.toString())
                            .onItem().ifNull().failWith(() -> Status.NOT_FOUND
                                    .withDescription("Document data not found in storage: " + documentId)
                                    .asRuntimeException())
                            .map(doc -> GetUploadedDocumentResponse.newBuilder()
                                    .setDocument(doc)
                                    .build());
                })
                .onFailure().invoke(e -> {
                    if (!(e instanceof io.grpc.StatusRuntimeException)) {
                        LOG.error("Failed to get document", e);
                    }
                });
    }
}