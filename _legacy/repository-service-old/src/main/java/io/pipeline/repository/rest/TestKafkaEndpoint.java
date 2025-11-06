package io.pipeline.repository.rest;

import io.pipeline.repository.filesystem.DriveEvent;
import io.pipeline.repository.filesystem.DocumentEvent;
import io.pipeline.repository.filesystem.SearchIndexEvent;
import io.pipeline.repository.filesystem.RequestCountEvent;
import io.pipeline.repository.kafka.DriveEventEmitter;
import io.pipeline.repository.kafka.DocumentEventEmitter;
import io.pipeline.repository.kafka.SearchIndexEventEmitter;
import io.pipeline.repository.kafka.RequestCountEventEmitter;
import io.pipeline.repository.services.EventPublisher;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

/**
 * Test endpoint for triggering Kafka events in integration tests.
 * 
 * This endpoint is only available in test/dev environments and provides
 * a way to trigger the emitters without needing the full gRPC service implementation.
 */
@Path("/test/kafka")
public class TestKafkaEndpoint {

    private static final Logger LOG = Logger.getLogger(TestKafkaEndpoint.class);

    @Inject
    DriveEventEmitter driveEventEmitter;
    
    @Inject
    DocumentEventEmitter documentEventEmitter;
    
    @Inject
    SearchIndexEventEmitter searchIndexEventEmitter;
    
    @Inject
    RequestCountEventEmitter requestCountEventEmitter;
    
    @Inject
    EventPublisher eventPublisher;

    @GET
    @Path("/drive-event")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerDriveEvent() {
        try {
            LOG.info("Triggering test DriveEvent");
            
            DriveEvent driveEvent = DriveEvent.newBuilder()
                    .setDriveId(12345L)
                    .setDriveName("test-drive")
                    .setCreated(DriveEvent.DriveCreated.newBuilder()
                            .setBucketName("test-bucket")
                            .setRegion("us-east-1")
                            .setDescription("Test drive for integration test")
                            .build())
                    .build();

            driveEventEmitter.sendAndForget(driveEvent);
            
            return Response.ok()
                    .entity("{\"status\": \"success\", \"message\": \"DriveEvent sent\", \"driveId\": 12345}")
                    .build();
                    
        } catch (Exception e) {
            LOG.error("Failed to send DriveEvent", e);
            return Response.serverError()
                    .entity("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }

    @GET
    @Path("/document-event")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerDocumentEvent() {
        try {
            LOG.info("Triggering test DocumentEvent");
            
            DocumentEvent documentEvent = DocumentEvent.newBuilder()
                    .setDocumentId("doc-integration-test-123")
                    .setDriveId(12345L)
                    .setPath("/test/test-document.pdf")
                    .setCreated(DocumentEvent.DocumentCreated.newBuilder()
                            .setName("test-document.pdf")
                            .setDocumentType("pdf")
                            .setContentType("application/pdf")
                            .setSize(1024L)
                            .setS3Key("test-key")
                            .build())
                    .build();

            documentEventEmitter.sendAndForget(documentEvent);
            
            return Response.ok()
                    .entity("{\"status\": \"success\", \"message\": \"DocumentEvent sent\", \"documentId\": \"doc-integration-test-123\"}")
                    .build();
                    
        } catch (Exception e) {
            LOG.error("Failed to send DocumentEvent", e);
            return Response.serverError()
                    .entity("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }

    @GET
    @Path("/search-index-event")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerSearchIndexEvent() {
        try {
            LOG.info("Triggering test SearchIndexEvent");
            
            SearchIndexEvent searchEvent = SearchIndexEvent.newBuilder()
                    .setDocumentId("doc-integration-test-123")
                    .setIndexName("documents")
                    .setIndexCompleted(SearchIndexEvent.SearchIndexCompleted.newBuilder()
                            .setIndexDocumentId("index-doc-123")
                            .setChunkCount(5)
                            .setEmbeddingCount(10)
                            .build())
                    .build();

            searchIndexEventEmitter.sendAndForget(searchEvent);
            
            return Response.ok()
                    .entity("{\"status\": \"success\", \"message\": \"SearchIndexEvent sent\", \"documentId\": \"doc-integration-test-123\"}")
                    .build();
                    
        } catch (Exception e) {
            LOG.error("Failed to send SearchIndexEvent", e);
            return Response.serverError()
                    .entity("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }

    @GET
    @Path("/request-count-event")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerRequestCountEvent() {
        try {
            LOG.info("Triggering test RequestCountEvent");
            
            RequestCountEvent.DocumentRequestCount docRequest = RequestCountEvent.DocumentRequestCount.newBuilder()
                    .setDocumentId("doc-integration-test-123")
                    .setOperation("READ")
                    .setBytesTransferred(1024L)
                    .setProcessingTimeMs(100L)
                    .build();
            
            RequestCountEvent requestCountEvent = RequestCountEvent.newBuilder()
                    .setDriveId(12345L)
                    .setDocumentRequest(docRequest)
                    .build();

            requestCountEventEmitter.sendAndForget(requestCountEvent);
            
            return Response.ok()
                    .entity("{\"status\": \"success\", \"message\": \"RequestCountEvent sent\", \"documentId\": \"doc-integration-test-123\"}")
                    .build();
                    
        } catch (Exception e) {
            LOG.error("Failed to send RequestCountEvent", e);
            return Response.serverError()
                    .entity("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }

    @GET
    @Path("/all-events")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerAllEvents() {
        try {
            LOG.info("Triggering all test events");
            
            // Send all event types
            triggerDriveEvent();
            triggerDocumentEvent();
            triggerSearchIndexEvent();
            triggerRequestCountEvent();
            
            return Response.ok()
                    .entity("{\"status\": \"success\", \"message\": \"All events sent\"}")
                    .build();
                    
        } catch (Exception e) {
            LOG.error("Failed to send all events", e);
            return Response.serverError()
                    .entity("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }
}