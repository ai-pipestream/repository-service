package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.events.v1.IntakeRepoEvent;
import ai.pipestream.events.v1.IntakeRepoEventType;
import ai.pipestream.repository.filesystem.v1.RepositoryEvent;
import ai.pipestream.repository.filesystem.v1.SourceContext;
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
 *
 * Uses the Apicurio extension for automatic Protobuf serialization.
 * Events are keyed by document_id (derived to UUID) for partition locality.
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

    private static final String INTAKE_SOURCE_NODE_ID = "connector-intake";

    /**
     * Emit a Created event after successful document storage.
     *
     * @param docId Document ID (used for key derivation)
     * @param accountId Account ID
     * @param s3Key S3 key for raw file
     * @param pipedocS3Key S3 key for PipeDoc protobuf
     * @param sizeBytes Size in bytes
     * @param contentHash SHA-256 hash (if available)
     * @param bucket S3 bucket name
     * @param versionId S3 version ID (if versioning enabled)
     * @param requestId Request ID for tracing
     * @param connectorId Connector ID (if connector-initiated)
     */
    public void emitCreated(String docId,
                            String accountId,
                            String s3Key,
                            String pipedocS3Key,
                            long sizeBytes,
                            String contentHash,
                            String bucket,
                            String versionId,
                            String requestId,
                            String connectorId) {
        emitCreated(docId, accountId, s3Key, pipedocS3Key, sizeBytes, contentHash, bucket, versionId,
            requestId, connectorId, null);
    }

    /**
     * Emit a Created event after successful document storage.
     *
     * Optionally emits an IntakeRepoEvent for connector-origin events.
     *
     * @param docId Document ID (used for key derivation)
     * @param accountId Account ID
     * @param s3Key S3 key for raw file
     * @param pipedocS3Key S3 key for PipeDoc protobuf
     * @param sizeBytes Size in bytes
     * @param contentHash SHA-256 hash (if available)
     * @param bucket S3 bucket name
     * @param versionId S3 version ID (if versioning enabled)
     * @param requestId Request ID for tracing
     * @param connectorId Connector ID (if connector-initiated)
     * @param datasourceId Datasource ID (if known for IntakeRepoEvent emission)
     */
    public void emitCreated(String docId,
                            String accountId,
                            String s3Key,
                            String pipedocS3Key,
                            long sizeBytes,
                            String contentHash,
                            String bucket,
                            String versionId,
                            String requestId,
                            String connectorId,
                            String datasourceId) {

        Instant now = Instant.now();
        String eventId = computeEventId(docId, "create", now);

        RepositoryEvent.Created.Builder created = RepositoryEvent.Created.newBuilder()
                .setS3Key(s3Key)
                .setSize(sizeBytes)
                .setBucketName(bucket);

        if (contentHash != null && !contentHash.isEmpty()) {
            created.setContentHash(contentHash);
        }
        if (versionId != null && !versionId.isEmpty()) {
            created.setS3VersionId(versionId);
        }

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("create")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());

        if (connectorId != null && !connectorId.isEmpty()) {
            source.setConnectorId(connectorId);
        }

        RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setCreated(created.build())
                .build();

        // ProtobufEmitter uses RepositoryEventKeyExtractor to derive UUID key from document_id
        emitter.send(event);
        LOG.debugf("Emitted RepositoryEvent.Created: docId=%s, s3Key=%s", docId, s3Key);

        if (connectorId != null && !connectorId.isBlank() && datasourceId != null && !datasourceId.isBlank()) {
            emitIntakeRepoCreated(eventId, now, docId, accountId, connectorId, datasourceId, versionId);
        }
    }

    private void emitIntakeRepoCreated(String eventId,
                                       Instant now,
                                       String docId,
                                       String accountId,
                                       String connectorId,
                                       String datasourceId,
                                       String versionId) {
        IntakeRepoEvent intakeEvent = IntakeRepoEvent.newBuilder()
                .setEventId(eventId + "-intake")
                .setEventType(IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_CREATED)
                .setEventTime(toProtoTimestamp(now))
                .setDocId(docId)
                .setAccountId(accountId)
                .setConnectorId(connectorId)
                .setDatasourceId(datasourceId)
                .setSourceNodeId(INTAKE_SOURCE_NODE_ID)
                .setS3VersionId(versionId == null ? "" : versionId)
                .build();

        intakeEventEmitter.send(intakeEvent);
        LOG.infof("Emitted IntakeRepoEvent created for connector handoff: docId=%s, accountId=%s, datasourceId=%s",
                docId, accountId, datasourceId);
    }

    /**
     * Emit an Updated event after successful document update.
     */
    public void emitUpdated(String docId,
                            String accountId,
                            String s3Key,
                            long sizeBytes,
                            String contentHash,
                            String bucket,
                            String newVersionId,
                            String previousVersionId,
                            String requestId,
                            String connectorId) {

        Instant now = Instant.now();
        String eventId = computeEventId(docId, "update", now);

        RepositoryEvent.Updated.Builder updated = RepositoryEvent.Updated.newBuilder()
                .setS3Key(s3Key)
                .setSize(sizeBytes)
                .setBucketName(bucket);

        if (contentHash != null && !contentHash.isEmpty()) {
            updated.setContentHash(contentHash);
        }
        if (newVersionId != null && !newVersionId.isEmpty()) {
            updated.setS3VersionId(newVersionId);
        }
        if (previousVersionId != null && !previousVersionId.isEmpty()) {
            updated.setPreviousVersionId(previousVersionId);
        }

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("update")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());

        if (connectorId != null && !connectorId.isEmpty()) {
            source.setConnectorId(connectorId);
        }

        RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setUpdated(updated.build())
                .build();

        emitter.send(event);
        LOG.infof("Emitted RepositoryEvent.Updated: docId=%s, s3Key=%s", docId, s3Key);
    }

    /**
     * Emit a Deleted event after successful document deletion.
     */
    public void emitDeleted(String docId,
                            String accountId,
                            String reason,
                            boolean purged,
                            String requestId,
                            String connectorId) {

        Instant now = Instant.now();
        String eventId = computeEventId(docId, "delete", now);

        RepositoryEvent.Deleted.Builder deleted = RepositoryEvent.Deleted.newBuilder()
                .setPurged(purged);

        if (reason != null && !reason.isEmpty()) {
            deleted.setReason(reason);
        }

        SourceContext.Builder source = SourceContext.newBuilder()
                .setComponent(COMPONENT)
                .setOperation("delete")
                .setRequestId(requestId != null ? requestId : UUID.randomUUID().toString());

        if (connectorId != null && !connectorId.isEmpty()) {
            source.setConnectorId(connectorId);
        }

        RepositoryEvent event = RepositoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(toProtoTimestamp(now))
                .setDocumentId(docId)
                .setAccountId(accountId)
                .setSource(source.build())
                .setDeleted(deleted.build())
                .build();

        emitter.send(event);
        LOG.infof("Emitted RepositoryEvent.Deleted: docId=%s, purged=%s", docId, purged);
    }

    /**
     * Compute deterministic event ID: first 32 chars of SHA-256(docId + operation + timestamp_millis).
     */
    private static String computeEventId(String docId, String operation, Instant timestamp) {
        String input = docId + ":" + operation + ":" + timestamp.toEpochMilli();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is always available
            return UUID.randomUUID().toString().replace("-", "");
        }
    }

    private static Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
