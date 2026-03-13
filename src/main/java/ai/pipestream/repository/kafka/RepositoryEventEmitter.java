package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.events.v1.IntakeRepoEvent;
import ai.pipestream.events.v1.IntakeRepoEventType;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import ai.pipestream.repository.filesystem.v1.SourceContext;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.data.v1.OwnershipContext;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Emits repository events to Kafka for downstream consumers.
 */
@ApplicationScoped
public class RepositoryEventEmitter {

    private static final Logger LOG = Logger.getLogger(RepositoryEventEmitter.class);
    private static final String COMPONENT = "repository-service";

    @Inject
    @ProtobufChannel("repository-events-out")
    ProtobufEmitter<RepositoryEvent> emitter;

    @Inject
    @ProtobufChannel("intake-repo-events-out")
    ProtobufEmitter<IntakeRepoEvent> intakeEventEmitter;

    @Inject
    @ProtobufChannel("pipedoc-updates-out")
    ProtobufEmitter<PipeDocUpdateNotification> pipeDocUpdateEmitter;

    private static final String INTAKE_SOURCE_NODE_ID = "connector-intake";

    public void emitCreated(String docId, String accountId, String s3Key, String pipedocS3Key, long sizeBytes, String contentHash, String bucket, String versionId, String requestId, String connectorId) {
        emitCreated(docId, accountId, s3Key, pipedocS3Key, sizeBytes, contentHash, bucket, versionId, requestId, connectorId, null);
    }

    public void emitCreated(String docId, String accountId, String s3Key, String pipedocS3Key, long sizeBytes, String contentHash, String bucket, String versionId, String requestId, String connectorId, String datasourceId) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "create", now);
        RepositoryEvent.Created.Builder created = RepositoryEvent.Created.newBuilder().setS3Key(s3Key).setSize(sizeBytes).setBucketName(bucket);
        if (contentHash != null && !contentHash.isEmpty()) created.setContentHash(contentHash);
        if (versionId != null && !versionId.isEmpty()) created.setS3VersionId(versionId);
        SourceContext.Builder source = SourceContext.newBuilder().setComponent(COMPONENT).setOperation("create").setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);
        RepositoryEvent event = RepositoryEvent.newBuilder().setEventId(eventId).setTimestamp(toProtoTimestamp(now)).setDocumentId(docId).setAccountId(accountId).setSource(source.build()).setCreated(created.build()).build();
        emitter.send(event);
        if (connectorId != null && !connectorId.isBlank() && datasourceId != null && !datasourceId.isBlank()) {
            emitIntakeRepoCreated(eventId, now, docId, accountId, connectorId, datasourceId, versionId);
        }
    }

    private void emitIntakeRepoCreated(String eventId, Instant now, String docId, String accountId, String connectorId, String datasourceId, String versionId) {
        IntakeRepoEvent intakeEvent = IntakeRepoEvent.newBuilder().setEventId(eventId + "-intake").setEventType(IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_CREATED).setEventTime(toProtoTimestamp(now)).setDocId(docId).setAccountId(accountId).setConnectorId(connectorId).setDatasourceId(datasourceId).setSourceNodeId(INTAKE_SOURCE_NODE_ID).setS3VersionId(versionId == null ? "" : versionId).build();
        intakeEventEmitter.send(intakeEvent);
    }

    public void emitUpdated(String docId, String accountId, String s3Key, long sizeBytes, String contentHash, String bucket, String newVersionId, String previousVersionId, String requestId, String connectorId) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "update", now);
        RepositoryEvent.Updated.Builder updated = RepositoryEvent.Updated.newBuilder().setS3Key(s3Key).setSize(sizeBytes).setBucketName(bucket);
        if (contentHash != null && !contentHash.isEmpty()) updated.setContentHash(contentHash);
        if (newVersionId != null && !newVersionId.isEmpty()) updated.setS3VersionId(newVersionId);
        if (previousVersionId != null && !previousVersionId.isEmpty()) updated.setPreviousVersionId(previousVersionId);
        SourceContext.Builder source = SourceContext.newBuilder().setComponent(COMPONENT).setOperation("update").setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);
        RepositoryEvent event = RepositoryEvent.newBuilder().setEventId(eventId).setTimestamp(toProtoTimestamp(now)).setDocumentId(docId).setAccountId(accountId).setSource(source.build()).setUpdated(updated.build()).build();
        emitter.send(event);
    }

    public void emitDeleted(String docId, String accountId, String reason, boolean purged, String requestId, String connectorId) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "delete", now);
        RepositoryEvent.Deleted.Builder deleted = RepositoryEvent.Deleted.newBuilder().setPurged(purged);
        if (reason != null && !reason.isEmpty()) deleted.setReason(reason);
        SourceContext.Builder source = SourceContext.newBuilder().setComponent(COMPONENT).setOperation("delete").setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);
        RepositoryEvent event = RepositoryEvent.newBuilder().setEventId(eventId).setTimestamp(toProtoTimestamp(now)).setDocumentId(docId).setAccountId(accountId).setSource(source.build()).setDeleted(deleted.build()).build();
        emitter.send(event);
    }

    /**
     * Emit a PipeDocUpdateNotification with full ownership context.
     */
    public void emitPipeDocUpdate(String updateType, String storageId, String docId, String title, String author, OwnershipContext ownership) {
        Instant now = Instant.now();
        PipeDocUpdateNotification.Builder builder = PipeDocUpdateNotification.newBuilder()
                .setUpdateType(updateType)
                .setStorageId(storageId != null ? storageId : "")
                .setDocId(docId != null ? docId : "")
                .setTimestamp(now.toEpochMilli())
                .setCreatedAt(toProtoTimestamp(now))
                .setUpdatedAt(toProtoTimestamp(now));

        if (title != null && !title.isBlank()) builder.setTitle(title);
        if (author != null && !author.isBlank()) builder.setAuthor(author);
        if (ownership != null) builder.setOwnership(ownership);

        try {
            pipeDocUpdateEmitter.send(builder.build());
            LOG.debugf("Emitted PipeDocUpdateNotification: type=%s, storageId=%s, docId=%s", updateType, storageId, docId);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit PipeDocUpdateNotification for docId=%s", docId);
        }
    }

    private static String computeEventId(String docId, String operation, Instant timestamp) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest((docId + ":" + operation + ":" + timestamp.toEpochMilli()).getBytes(StandardCharsets.UTF_8))).substring(0, 32);
        } catch (NoSuchAlgorithmException e) { return UUID.randomUUID().toString().replace("-", ""); }
    }

    private static Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
