package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.events.v1.DocumentUploadedEvent;
import ai.pipestream.events.v1.IntakeRepoEvent;
import ai.pipestream.events.v1.IntakeRepoEventType;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import ai.pipestream.repository.filesystem.v1.SourceContext;
import ai.pipestream.repository.v1.PipeDocUpdateNotification;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.repository.v1.CacheFlushEvent;
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

    @Inject
    @ProtobufChannel("document-uploaded-events-out")
    ProtobufEmitter<DocumentUploadedEvent> documentUploadedEmitter;

    @Inject
    @ProtobufChannel("cache-flush-out")
    ProtobufEmitter<CacheFlushEvent> cacheFlushEmitter;

    // source_node_id on IntakeRepoEvent is set to datasourceId (the graph entry point),
    // not a service name. The intake consumer uses datasourceId for engine handoff.

    public void emitCreated(String docId, String accountId, String storageKey, String pipedocStorageKey, long sizeBytes, String contentHash, String driveName, String versionId, String requestId, String connectorId) {
        emitCreated(docId, accountId, storageKey, pipedocStorageKey, sizeBytes, contentHash, driveName, versionId, requestId, connectorId, null, null);
    }

    /**
     * Emits a storage intent event BEFORE the S3 upload begins.
     * If the upload or DB persist fails, this event marks an orphan candidate.
     * Uses the same Kafka key as the subsequent Created event so ordering is preserved.
     */
    public void emitStorageIntent(String docId, String accountId, String storageKey,
                                   String driveName, String requestId, String connectorId,
                                   String datasourceId, OwnershipContext ownership) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "intent", now);
        RepositoryEvent.Created.Builder created = RepositoryEvent.Created.newBuilder()
                .setStorageKey(storageKey)
                .setDriveName(driveName != null ? driveName : "");

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("intent")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);

        RepositoryEvent.Builder eventBuilder = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setCreated(created.build());
        if (datasourceId != null && !datasourceId.isEmpty()) eventBuilder.setDatasourceId(datasourceId);
        if (ownership != null) eventBuilder.setOwnership(ownership);

        emitter.send(eventBuilder.build());
    }

    /** Backward-compatible overload without catalog metadata fields. */
    public void emitCreated(String docId, String accountId, String storageKey, String pipedocStorageKey,
                             long sizeBytes, String contentHash, String driveName, String versionId,
                             String requestId, String connectorId, String datasourceId) {
        emitCreated(docId, accountId, storageKey, sizeBytes, contentHash, driveName, versionId, null,
                requestId, connectorId, datasourceId, null, null, null, null);
    }

    /** Backward-compatible overload without catalog metadata fields. */
    public void emitCreated(String docId, String accountId, String storageKey, String pipedocStorageKey,
                             long sizeBytes, String contentHash, String driveName, String versionId,
                             String requestId, String connectorId, String datasourceId, OwnershipContext ownership) {
        emitCreated(docId, accountId, storageKey, sizeBytes, contentHash, driveName, versionId, null,
                requestId, connectorId, datasourceId, ownership, null, null, null);
    }

    /**
     * Emits the full Created event AFTER S3 upload + DB persist succeed.
     * Carries all catalog metadata so the opensearch-manager can index a complete entry.
     */
    public void emitCreated(String docId, String accountId, String storageKey, long sizeBytes,
                             String contentHash, String driveName, String versionId, String storageEtag,
                             String requestId, String connectorId, String datasourceId,
                             OwnershipContext ownership, String name, String path, String contentType) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "create", now);
        RepositoryEvent.Created.Builder created = RepositoryEvent.Created.newBuilder()
                .setStorageKey(storageKey)
                .setSize(sizeBytes)
                .setDriveName(driveName != null ? driveName : "");
        if (contentHash != null && !contentHash.isEmpty()) created.setContentHash(contentHash);
        if (versionId != null && !versionId.isEmpty()) created.setStorageVersionId(versionId);
        if (name != null && !name.isEmpty()) created.setName(name);
        if (path != null && !path.isEmpty()) created.setPath(path);
        if (contentType != null && !contentType.isEmpty()) created.setContentType(contentType);

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("create")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);

        RepositoryEvent.Builder eventBuilder = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setCreated(created.build());
        if (datasourceId != null && !datasourceId.isEmpty()) eventBuilder.setDatasourceId(datasourceId);
        if (ownership != null) eventBuilder.setOwnership(ownership);

        emitter.send(eventBuilder.build());
        // IntakeRepoEvent is NOT emitted here. It must only be emitted by the actual
        // intake entry point (RawUploadResource) to prevent infinite loops when the engine
        // saves intermediate pipeline state via savePipeDoc.
    }

    /**
     * Emits a DocumentUploadedEvent tracking raw binary data arrival in storage.
     */
    public void emitDocumentUploaded(String docId, String accountId, String s3Key, String connectorId,
                                      String filename, String mimeType, String path,
                                      Instant creationDate, Instant lastModifiedDate) {
        try {
            DocumentUploadedEvent.Builder builder = DocumentUploadedEvent.newBuilder()
                    .setDocId(docId)
                    .setS3Key(s3Key)
                    .setAccountId(accountId);

            if (connectorId != null && !connectorId.isEmpty()) builder.setConnectorId(connectorId);
            if (filename != null && !filename.isEmpty()) builder.setFilename(filename);
            if (mimeType != null && !mimeType.isEmpty()) builder.setMimeType(mimeType);
            if (path != null && !path.isEmpty()) builder.setPath(path);
            if (creationDate != null) builder.setCreationDate(toProtoTimestamp(creationDate));
            if (lastModifiedDate != null) builder.setLastModifiedDate(toProtoTimestamp(lastModifiedDate));

            documentUploadedEmitter.send(builder.build());
            LOG.debugf("Emitted DocumentUploadedEvent: docId=%s, filename=%s, mimeType=%s", docId, filename, mimeType);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit DocumentUploadedEvent for docId=%s", docId);
        }
    }

    public void emitIntakeRepoCreated(String eventId, Instant now, String docId, String accountId, String connectorId, String datasourceId, String versionId) {
        IntakeRepoEvent intakeEvent = IntakeRepoEvent.newBuilder().setEventId(eventId + "-intake").setEventType(IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_CREATED).setEventTime(toProtoTimestamp(now)).setDocId(docId).setAccountId(accountId).setConnectorId(connectorId).setDatasourceId(datasourceId).setSourceNodeId(datasourceId).setS3VersionId(versionId == null ? "" : versionId).build();
        intakeEventEmitter.send(intakeEvent);
    }

    public void emitUpdated(String docId, String accountId, String storageKey, long sizeBytes, String contentHash, String driveName, String newVersionId, String previousVersionId, String requestId, String connectorId) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "update", now);
        RepositoryEvent.Updated.Builder updated = RepositoryEvent.Updated.newBuilder()
                .setStorageKey(storageKey)
                .setSize(sizeBytes)
                .setDriveName(driveName != null ? driveName : "");
        if (contentHash != null && !contentHash.isEmpty()) updated.setContentHash(contentHash);
        if (newVersionId != null && !newVersionId.isEmpty()) updated.setStorageVersionId(newVersionId);
        if (previousVersionId != null && !previousVersionId.isEmpty()) updated.setPreviousVersionId(previousVersionId);

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("update")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);

        RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setUpdated(updated.build())
                .build();
        emitter.send(event);
    }

    public void emitDeleted(String docId, String accountId, String reason, boolean purged, String requestId, String connectorId, String datasourceId) {
        Instant now = Instant.now();
        String eventId = computeEventId(docId, "delete", now);
        RepositoryEvent.Deleted.Builder deleted = RepositoryEvent.Deleted.newBuilder().setPurged(purged);
        if (reason != null && !reason.isEmpty()) deleted.setReason(reason);
        SourceContext.Builder source = SourceContext.newBuilder().setComponent(COMPONENT).setOperation("delete").setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());
        if (connectorId != null && !connectorId.isEmpty()) source.setConnectorId(connectorId);
        RepositoryEvent.Builder eventBuilder = RepositoryEvent.newBuilder().setEventId(eventId).setTimestamp(toProtoTimestamp(now)).setDocumentId(docId).setAccountId(accountId).setSource(source.build()).setDeleted(deleted.build());
        if (datasourceId != null && !datasourceId.isEmpty()) eventBuilder.setDatasourceId(datasourceId);
        emitter.send(eventBuilder.build());
    }

    /**
     * Emit a PipeDocUpdateNotification with full ownership context.
     */
    public void emitPipeDocUpdate(String updateType, String storageId, String docId, String title, String author, OwnershipContext ownership, int retentionIntentDays) {
        Instant now = Instant.now();
        PipeDocUpdateNotification.Builder builder = PipeDocUpdateNotification.newBuilder()
                .setUpdateType(updateType)
                .setStorageId(storageId != null ? storageId : "")
                .setDocId(docId != null ? docId : "")
                .setTimestamp(now.toEpochMilli())
                .setCreatedAt(toProtoTimestamp(now))
                .setUpdatedAt(toProtoTimestamp(now))
                .setRetentionIntentDays(retentionIntentDays);

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

    /**
     * Emits a cache flush event to Kafka for the background storage flusher.
     * The flusher reads the document from Redis and persists to durable storage.
     */
    public void emitCacheFlushEvent(String nodeId, String objectKey, String driveName, String accountId) {
        CacheFlushEvent event = CacheFlushEvent.newBuilder()
                .setNodeId(nodeId)
                .setObjectKey(objectKey)
                .setDriveName(driveName)
                .setAccountId(accountId)
                .build();
        cacheFlushEmitter.send(event);
        LOG.debugf("Emitted cache flush event for node_id=%s", nodeId);
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
