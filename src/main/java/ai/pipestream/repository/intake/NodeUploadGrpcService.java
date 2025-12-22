package ai.pipestream.repository.intake;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.config.FeatureFlags;
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
                    return UploadFilesystemPipeDocResponse.newBuilder()
                            .setSuccess(true)
                            .setDocumentId(stored.documentId())
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

        return storageService.get(documentId)
                .onItem().ifNull().failWith(() -> Status.NOT_FOUND
                        .withDescription("Document not found: " + documentId)
                        .asRuntimeException())
                .map(doc -> GetUploadedDocumentResponse.newBuilder()
                        .setDocument(doc)
                        .build())
                .onFailure().invoke(e -> {
                    if (!(e instanceof io.grpc.StatusRuntimeException)) {
                        LOG.error("Failed to get document", e);
                    }
                });
    }
}