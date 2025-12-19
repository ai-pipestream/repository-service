package ai.pipestream.repository.intake;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.config.FeatureFlags;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentRequest;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentResponse;
import ai.pipestream.repository.v1.filesystem.upload.NodeUploadServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Phase 1 gRPC service implementation for NodeUploadService.
 *
 * This is aligned to the current source-of-record repo protos (unary RPCs only):
 * - UploadPipeDoc
 * - GetDocument
 */
@GrpcService
public class NodeUploadGrpcService extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(NodeUploadGrpcService.class);

    @Inject
    FeatureFlags featureFlags;

    @Inject
    IntakeMetrics metrics;

    @Inject
    DocumentStorageService storageService;

    @Override
    public void uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request,
                              StreamObserver<UploadFilesystemPipeDocResponse> responseObserver) {
        Timer.Sample timerSample = metrics.startInitiateUploadTimer();

        try {
            if (!featureFlags.phase1().enabled()) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Phase 1 intake service is not enabled")
                        .asRuntimeException());
                return;
            }

            if (request == null || !request.hasDocument()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("document is required")
                        .asRuntimeException());
                return;
            }

            PipeDoc document = request.getDocument();
            DocumentStorageService.StoredDocument stored = storageService.store(document);

            metrics.recordUploadInitiated();

            UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(stored.documentId())
                    .setS3Key(stored.s3Key())
                    .setMessage("stored")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("Failed to upload PipeDoc", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to upload PipeDoc: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            metrics.stopInitiateUploadTimer(timerSample);
        }
    }

    @Override
    public void getUploadedDocument(GetUploadedDocumentRequest request,
                            StreamObserver<GetUploadedDocumentResponse> responseObserver) {
        try {
            if (request == null || request.getDocumentId() == null || request.getDocumentId().isBlank()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("document_id is required")
                        .asRuntimeException());
                return;
            }

            String documentId = request.getDocumentId();

            var docOpt = storageService.get(documentId);
            if (docOpt.isEmpty()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Document not found: " + documentId)
                        .asRuntimeException());
                return;
            }

            GetUploadedDocumentResponse response = GetUploadedDocumentResponse.newBuilder()
                    .setDocument(docOpt.get())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("Failed to get document", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get document: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
