package ai.pipestream.repository.intake;

import ai.pipestream.repository.config.FeatureFlags;
import ai.pipestream.repository.config.IntakeConfiguration;
import ai.pipestream.repository.filesystem.upload.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Phase 1 gRPC service implementation for NodeUploadService.
 * Provides fast ACK pattern with in-memory state only (no persistence).
 */
@GrpcService
public class NodeUploadGrpcService extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(NodeUploadGrpcService.class);

    @Inject
    FeatureFlags featureFlags;

    @Inject
    IntakeConfiguration config;

    @Inject
    InMemoryUploadStateStore stateStore;

    @Inject
    IntakeMetrics metrics;

    private final ScheduledExecutorService progressScheduler = Executors.newScheduledThreadPool(2);

    @Override
    public void initiateUpload(InitiateUploadRequest request,
                               StreamObserver<InitiateUploadResponse> responseObserver) {
        Timer.Sample timerSample = metrics.startInitiateUploadTimer();

        try {
            if (!featureFlags.phase1().enabled()) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Phase 1 intake service is not enabled")
                        .asRuntimeException());
                return;
            }

            // Generate node_id and upload_id
            String nodeId = !request.getClientNodeId().isEmpty()
                    ? request.getClientNodeId()
                    : UUID.randomUUID().toString();
            String uploadId = UUID.randomUUID().toString();

            // Set up logging context
            MDC.put("nodeId", nodeId);
            MDC.put("uploadId", uploadId);
            MDC.put("traceId", Span.current().getSpanContext().getTraceId());

            LOG.infof("Initiating upload: nodeId=%s, uploadId=%s, name=%s, driveId=%s",
                    nodeId, uploadId, request.getName(), request.getDrive());

            // Create in-memory state
            stateStore.create(nodeId, uploadId);

            // Record metrics
            metrics.recordUploadInitiated();

            // Build response
            InitiateUploadResponse response = InitiateUploadResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setUploadId(uploadId)
                    .setState(UploadState.UPLOAD_STATE_UPLOADING)
                    .setCreatedAtEpochMs(System.currentTimeMillis())
                    .setIsUpdate(false)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            LOG.infof("Upload initiated successfully: nodeId=%s, uploadId=%s", nodeId, uploadId);

        } catch (Exception e) {
            LOG.errorf(e, "Failed to initiate upload");
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to initiate upload: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            metrics.stopInitiateUploadTimer(timerSample);
            MDC.clear();
        }
    }

    @Override
    public void uploadChunk(UploadChunkRequest request,
                            StreamObserver<UploadChunkResponse> responseObserver) {
        Timer.Sample timerSample = metrics.startUploadChunkTimer();

        try {
            if (!featureFlags.phase1().enabled()) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Phase 1 intake service is not enabled")
                        .asRuntimeException());
                return;
            }

            String uploadId = request.getUploadId();
            String nodeId = request.getNodeId();
            int chunkNumber = (int) request.getChunkNumber();
            byte[] data = request.getData().toByteArray();
            int byteCount = data.length;

            // Set up logging context
            MDC.put("nodeId", nodeId);
            MDC.put("uploadId", uploadId);
            MDC.put("chunkNumber", String.valueOf(chunkNumber));
            MDC.put("traceId", Span.current().getSpanContext().getTraceId());

            // Validation
            if (chunkNumber < 0) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("chunk_number must be >= 0")
                        .asRuntimeException());
                return;
            }

            if (data == null || byteCount == 0) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("data payload cannot be null or empty")
                        .asRuntimeException());
                return;
            }

            // Get upload state
            Optional<InMemoryUploadState> stateOpt = stateStore.get(uploadId);
            if (stateOpt.isEmpty()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Upload not found: " + uploadId)
                        .asRuntimeException());
                return;
            }

            InMemoryUploadState state = stateOpt.get();

            // Idempotency check
            if (state.hasChunk(chunkNumber)) {
                // Duplicate chunk without force flag - return OK without reprocessing
                LOG.debugf("Idempotent hit: uploadId=%s, chunkNumber=%d", uploadId, chunkNumber);
                metrics.recordIdempotentHit();

                UploadChunkResponse response = UploadChunkResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setState(UploadState.UPLOAD_STATE_UPLOADING)
                        .setBytesUploaded(state.getBytesReceivedTotal())
                        .setChunkNumber(chunkNumber)
                        .setIsFileComplete(request.getIsLast())
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // Process the chunk
            boolean added = state.receiveChunk(chunkNumber, byteCount);

            LOG.debugf("Chunk received: uploadId=%s, chunkNumber=%d, bytes=%d, totalBytes=%d, totalChunks=%d",
                    uploadId, chunkNumber, byteCount, state.getBytesReceivedTotal(), state.getReceivedChunkCount());

            // Record metrics
            metrics.recordChunkReceived(byteCount);

            // Build response
            UploadChunkResponse response = UploadChunkResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setState(UploadState.UPLOAD_STATE_UPLOADING)
                    .setBytesUploaded(state.getBytesReceivedTotal())
                    .setChunkNumber(chunkNumber)
                    .setIsFileComplete(request.getIsLast())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.errorf(e, "Failed to process chunk");
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to process chunk: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            metrics.stopUploadChunkTimer(timerSample);
            MDC.clear();
        }
    }

    @Override
    public void getUploadStatus(GetUploadStatusRequest request,
                                StreamObserver<GetUploadStatusResponse> responseObserver) {
        Timer.Sample timerSample = metrics.startGetStatusTimer();

        try {
            if (!featureFlags.phase1().enabled()) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Phase 1 intake service is not enabled")
                        .asRuntimeException());
                return;
            }

            String nodeId = request.getNodeId();
            MDC.put("nodeId", nodeId);
            MDC.put("traceId", Span.current().getSpanContext().getTraceId());

            // In Phase 1, we look up by nodeId but the state is keyed by uploadId
            // We need to search for the state - for now, we use nodeId as a proxy
            // In a real implementation, we'd have an index from nodeId to uploadId

            // For Phase 1, return synthetic status
            Optional<InMemoryUploadState> stateOpt = findStateByNodeId(nodeId);

            if (stateOpt.isEmpty()) {
                // Return UNKNOWN status for not found uploads
                GetUploadStatusResponse response = GetUploadStatusResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setState(UploadState.UPLOAD_STATE_UNSPECIFIED)
                        .setBytesUploaded(0)
                        .setTotalBytes(0)
                        .setErrorMessage("Upload not found")
                        .setUpdatedAtEpochMs(System.currentTimeMillis())
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            InMemoryUploadState state = stateOpt.get();
            state.touch(); // Update last activity

            GetUploadStatusResponse response = GetUploadStatusResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setState(UploadState.UPLOAD_STATE_UPLOADING) // Always UPLOADING in Phase 1
                    .setBytesUploaded(state.getBytesReceivedTotal())
                    .setTotalBytes(0) // Unknown in Phase 1
                    .setUpdatedAtEpochMs(state.getLastActivityTs().toEpochMilli())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            LOG.debugf("Status check: nodeId=%s, bytesReceived=%d, chunksReceived=%d",
                    nodeId, state.getBytesReceivedTotal(), state.getReceivedChunkCount());

        } catch (Exception e) {
            LOG.errorf(e, "Failed to get upload status");
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get upload status: " + e.getMessage())
                    .asRuntimeException());
        } finally {
            metrics.stopGetStatusTimer(timerSample);
            MDC.clear();
        }
    }

    @Override
    public void streamUploadProgress(UploadProgressRequest request,
                                     StreamObserver<UploadProgressResponse> responseObserver) {
        if (!featureFlags.phase1().enabled()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Phase 1 intake service is not enabled")
                    .asRuntimeException());
            return;
        }

        String nodeId = request.getNodeId();
        MDC.put("nodeId", nodeId);
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());

        LOG.infof("Starting progress stream: nodeId=%s", nodeId);
        metrics.recordStreamClient();

        Optional<InMemoryUploadState> stateOpt = findStateByNodeId(nodeId);
        if (stateOpt.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Upload not found: " + nodeId)
                    .asRuntimeException());
            return;
        }

        InMemoryUploadState state = stateOpt.get();
        Duration interval = config.progress().interval();

        // Schedule periodic progress updates
        var future = progressScheduler.scheduleAtFixedRate(() -> {
            try {
                // Check if state still exists
                Optional<InMemoryUploadState> currentState = findStateByNodeId(nodeId);
                if (currentState.isEmpty()) {
                    LOG.debugf("Upload state no longer exists, ending stream: nodeId=%s", nodeId);
                    responseObserver.onCompleted();
                    return;
                }

                InMemoryUploadState s = currentState.get();
                UploadProgressResponse progress = UploadProgressResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setState(UploadState.UPLOAD_STATE_UPLOADING)
                        .setBytesUploaded(s.getBytesReceivedTotal())
                        .setTotalBytes(0) // Unknown in Phase 1
                        .setPercent(0.0) // Cannot calculate without total
                        .setUpdatedAtEpochMs(s.getLastActivityTs().toEpochMilli())
                        .build();

                responseObserver.onNext(progress);

            } catch (Exception e) {
                LOG.errorf(e, "Error sending progress update");
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Error sending progress update: " + e.getMessage())
                        .asRuntimeException());
            }
        }, 0, interval.toMillis(), TimeUnit.MILLISECONDS);

        // Note: The stream will run until client cancels or error occurs
        // In a production implementation, we'd want to track this future and cancel on client disconnect
    }

    @Override
    public StreamObserver<UploadChunkRequest> uploadChunks(StreamObserver<UploadChunkResponse> responseObserver) {
        if (!featureFlags.phase1().enabled()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Phase 1 intake service is not enabled")
                    .asRuntimeException());
            return new StreamObserver<UploadChunkRequest>() {
                @Override
                public void onNext(UploadChunkRequest value) {}
                @Override
                public void onError(Throwable t) {}
                @Override
                public void onCompleted() {}
            };
        }

        return new StreamObserver<UploadChunkRequest>() {
            @Override
            public void onNext(UploadChunkRequest request) {
                // Process each chunk in the stream
                uploadChunk(request, new StreamObserver<UploadChunkResponse>() {
                    @Override
                    public void onNext(UploadChunkResponse value) {
                        responseObserver.onNext(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        // Log but don't propagate to main stream yet
                        LOG.errorf(t, "Error processing chunk in stream");
                    }

                    @Override
                    public void onCompleted() {
                        // Individual chunk completed, don't complete the main stream
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                LOG.errorf(t, "Client stream error");
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                LOG.debug("Client stream completed");
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void cancelUpload(CancelUploadRequest request,
                             StreamObserver<CancelUploadResponse> responseObserver) {
        if (!featureFlags.phase1().enabled()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Phase 1 intake service is not enabled")
                    .asRuntimeException());
            return;
        }

        String nodeId = request.getNodeId();
        MDC.put("nodeId", nodeId);
        MDC.put("traceId", Span.current().getSpanContext().getTraceId());

        LOG.infof("Cancelling upload: nodeId=%s", nodeId);

        Optional<InMemoryUploadState> stateOpt = findStateByNodeId(nodeId);
        if (stateOpt.isEmpty()) {
            CancelUploadResponse response = CancelUploadResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setSuccess(false)
                    .setMessage("Upload not found")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        // Remove from state store
        String uploadId = stateOpt.get().getUploadId();
        stateStore.remove(uploadId);

        CancelUploadResponse response = CancelUploadResponse.newBuilder()
                .setNodeId(nodeId)
                .setSuccess(true)
                .setMessage("Upload cancelled successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        LOG.infof("Upload cancelled: nodeId=%s, uploadId=%s", nodeId, uploadId);
        MDC.clear();
    }

    /**
     * Helper method to find state by nodeId.
     */
    private Optional<InMemoryUploadState> findStateByNodeId(String nodeId) {
        return stateStore.getByNodeId(nodeId);
    }
}

