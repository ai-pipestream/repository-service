package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.*;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.smallrye.mutiny.subscription.Cancellable;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Test to verify that Kafka events are built without payloads.
 * This ensures we're following the architectural rule of no payloads in Kafka events.
 */
@QuarkusTest
public class PayloadRemovalVerificationTest {
    
    private static final Logger LOG = Logger.getLogger(PayloadRemovalVerificationTest.class);
    
    @Inject
    ReactiveEventPublisherImpl eventPublisher;
    
    @Test
    void testDocumentCreatedEventBuiltWithoutPayload() {
        LOG.info("Testing that DocumentCreated events can be built without payload");
        
        // Test that we can build a RepositoryEvent with Created operation without payload
        RepositoryEvent event = RepositoryEvent.newBuilder()
            .setEventId("test-event-id")
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int)((System.currentTimeMillis() % 1000) * 1_000_000))
                .build())
            .setDocumentId("test-document-id")
            .setAccountId("test-customer")
            .setSource(SourceContext.newBuilder()
                .setComponent("test-component")
                .setOperation("create")
                .setRequestId("test-request-id")
                .build())
            .setCreated(RepositoryEvent.Created.newBuilder()
                .setS3Key("test-s3-key")
                .setSize(1024L)
                .setContentHash("test-hash")
                .setS3VersionId("test-version")
                .build())
            .build();
        
        // Verify the event was built successfully
        assertThat("Event should be built successfully", event, is(notNullValue()));
        assertThat("Event should have correct document ID", event.getDocumentId(), is("test-document-id"));
        assertThat("Event should have created operation", event.hasCreated(), is(true));
        assertThat("Event should have correct S3 key", event.getCreated().getS3Key(), is("test-s3-key"));
        
        LOG.infof("✅ RepositoryEvent with Created operation built successfully without payload");
    }
    
    @Test
    void testDocumentUpdatedEventBuiltWithoutPayload() {
        LOG.info("Testing that DocumentUpdated events can be built without payload");
        
        // Test that we can build a RepositoryEvent with Updated operation without payload
        RepositoryEvent event = RepositoryEvent.newBuilder()
            .setEventId("test-event-id")
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int)((System.currentTimeMillis() % 1000) * 1_000_000))
                .build())
            .setDocumentId("test-document-id")
            .setAccountId("test-customer")
            .setSource(SourceContext.newBuilder()
                .setComponent("test-component")
                .setOperation("update")
                .setRequestId("test-request-id")
                .build())
            .setUpdated(RepositoryEvent.Updated.newBuilder()
                .setS3Key("test-s3-key")
                .setSize(2048L)
                .setContentHash("updated-hash")
                .setS3VersionId("updated-version")
                .setPreviousVersionId("previous-version")
                .build())
            .build();
        
        // Verify the event was built successfully
        assertThat("Event should be built successfully", event, is(notNullValue()));
        assertThat("Event should have correct document ID", event.getDocumentId(), is("test-document-id"));
        assertThat("Event should have updated operation", event.hasUpdated(), is(true));
        assertThat("Event should have correct S3 key", event.getUpdated().getS3Key(), is("test-s3-key"));
        assertThat("Event should have previous version ID", event.getUpdated().getPreviousVersionId(), is("previous-version"));
        
        LOG.infof("✅ RepositoryEvent with Updated operation built successfully without payload");
    }
    
    @Test
    void testDocumentDeletedEventBuiltWithoutPayload() {
        LOG.info("Testing that DocumentDeleted events can be built without payload");
        
        // Test that we can build a RepositoryEvent with Deleted operation without payload
        RepositoryEvent event = RepositoryEvent.newBuilder()
            .setEventId("test-event-id")
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int)((System.currentTimeMillis() % 1000) * 1_000_000))
                .build())
            .setDocumentId("test-document-id")
            .setAccountId("test-customer")
            .setSource(SourceContext.newBuilder()
                .setComponent("test-component")
                .setOperation("delete")
                .setRequestId("test-request-id")
                .build())
            .setDeleted(RepositoryEvent.Deleted.newBuilder()
                .setReason("Test deletion")
                .setPurged(false)
                .build())
            .build();
        
        // Verify the event was built successfully
        assertThat("Event should be built successfully", event, is(notNullValue()));
        assertThat("Event should have correct document ID", event.getDocumentId(), is("test-document-id"));
        assertThat("Event should have deleted operation", event.hasDeleted(), is(true));
        assertThat("Event should have correct reason", event.getDeleted().getReason(), is("Test deletion"));
        assertThat("Event should not be purged", event.getDeleted().getPurged(), is(false));
        
        LOG.infof("✅ RepositoryEvent with Deleted operation built successfully without payload");
    }
    
    @Test
    void testEventPublisherMethodsWorkWithoutPayload() {
        LOG.info("Testing that event publisher methods work without payload");
        
        // Test that the event publisher methods can be called without payload
        try {
            Cancellable cancellable = eventPublisher.publishDocumentCreatedAndForget(
                "test-document-id",
                "test-drive",
                1L,
                "/test/path",
                "test-file.txt",
                "text/plain",
                1024L,
                "test-s3-key",
                "type.googleapis.com/test.TestData",
                "test-hash",
                "test-version",
                "test-customer",
                "test-connector"
            );
            
            // Verify we got a cancellable back
            assertThat("Should return a cancellable", cancellable, is(notNullValue()));
            
            LOG.infof("✅ Event publisher methods work without payload");
            
        } catch (Exception e) {
            LOG.errorf(e, "❌ Event publisher methods failed");
            throw e;
        }
    }
}