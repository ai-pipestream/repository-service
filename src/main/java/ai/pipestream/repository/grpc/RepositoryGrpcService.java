package ai.pipestream.repository.grpc;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.grpc.util.KafkaProtobufKeys;
import ai.pipestream.repository.filesystem.RepositoryEvent;
import ai.pipestream.repository.filesystem.SourceContext;
import ai.pipestream.repository.pipedoc.GetPipeDocRequest;
import ai.pipestream.repository.pipedoc.GetPipeDocResponse;
import ai.pipestream.repository.pipedoc.ListPipeDocsRequest;
import ai.pipestream.repository.pipedoc.ListPipeDocsResponse;
import ai.pipestream.repository.pipedoc.PipeDocService;
import ai.pipestream.repository.pipedoc.SavePipeDocRequest;
import ai.pipestream.repository.pipedoc.SavePipeDocResponse;
import ai.pipestream.repository.service.DocumentStorageService;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

/**
 * gRPC service implementation for Repository Service.
 * Provides remote access to repository operations.
 */
@GrpcService
public class RepositoryGrpcService implements PipeDocService {

        private static final Logger LOG = Logger.getLogger(RepositoryGrpcService.class);

        @Inject
        DocumentStorageService storageService;

        @Inject
        @Channel("repository-events")
        MutinyEmitter<Record<UUID, RepositoryEvent>> eventEmitter;

        @Override
        @jakarta.transaction.Transactional
        public Uni<SavePipeDocResponse> savePipeDoc(SavePipeDocRequest request) {
                String driveName = request.getDrive();
                String connectorId = request.getConnectorId();
                PipeDoc pipeDoc = request.getPipedoc();
                String docId = pipeDoc.getDocId();

                // Validate Drive
                return Uni.createFrom().item(() -> ai.pipestream.repository.entity.Drive.findByName(driveName))
                                .onItem().ifNull().failWith(() -> new io.grpc.StatusRuntimeException(
                                                io.grpc.Status.NOT_FOUND
                                                                .withDescription("Drive not found: " + driveName)))
                                .onItem().transformToUni(drive -> {
                                        // Use Drive's bucket
                                        return storageService.uploadPipeDoc(drive.bucketName, connectorId, pipeDoc)
                                                        .onItem().transformToUni(etag -> {
                                                                // Create RepositoryEvent with Drive metadata
                                                                RepositoryEvent event = createCreatedEvent(drive,
                                                                                connectorId, docId, etag,
                                                                                pipeDoc.toByteArray().length);
                                                                UUID key = KafkaProtobufKeys.uuid(event);

                                                                // Emit event
                                                                return eventEmitter.send(Record.of(key, event))
                                                                                .replaceWith(() -> {
                                                                                        LOG.infof("Emitted RepositoryEvent for doc: %s (Drive: %s)",
                                                                                                        docId,
                                                                                                        drive.name);
                                                                                        return SavePipeDocResponse
                                                                                                        .newBuilder()
                                                                                                        .setNodeId(docId)
                                                                                                        .setDrive(drive.name)
                                                                                                        .setS3Key(buildS3Key(
                                                                                                                        connectorId,
                                                                                                                        docId))
                                                                                                        .setSizeBytes(pipeDoc
                                                                                                                        .toByteArray().length)
                                                                                                        .setChecksum(etag)
                                                                                                        .setCreatedAtEpochMs(
                                                                                                                        System.currentTimeMillis())
                                                                                                        .build();
                                                                                });
                                                        });
                                });
        }

        @Inject
        ai.pipestream.repository.service.MetadataService metadataService;

        @Override
        public Uni<GetPipeDocResponse> getPipeDoc(GetPipeDocRequest request) {
                String docId = request.getNodeId();

                return metadataService.getMetadata(docId)
                                .onItem().ifNull()
                                .failWith(() -> new io.grpc.StatusRuntimeException(
                                                io.grpc.Status.NOT_FOUND.withDescription("Document not found")))
                                .onItem()
                                .transformToUni(doc -> storageService
                                                .downloadPipeDoc(doc.bucketName, doc.connectorId, doc.documentId)
                                                .map(pipeDoc -> GetPipeDocResponse.newBuilder()
                                                                .setPipedoc(pipeDoc)
                                                                .build()));
        }

        @Override
        public Uni<ListPipeDocsResponse> listPipeDocs(ListPipeDocsRequest request) {
                return Uni.createFrom().failure(new UnsupportedOperationException("ListPipeDocs not implemented"));
        }

        private RepositoryEvent createCreatedEvent(ai.pipestream.repository.entity.Drive drive, String connectorId,
                        String docId, String etag, int size) {
                String s3Key = buildS3Key(connectorId, docId);
                Instant now = Instant.now();

                RepositoryEvent.Created created = RepositoryEvent.Created.newBuilder()
                                .setS3Key(s3Key)
                                .setSize(size)
                                .setContentHash(etag)
                                .setBucketName(drive.bucketName)
                                .build();

                return RepositoryEvent.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                                                .setNanos(now.getNano()).build())
                                .setDocumentId(docId)
                                .setAccountId(drive.accountId)
                                .setSource(SourceContext.newBuilder()
                                                .setComponent("repository-service")
                                                .setOperation("create")
                                                .setRequestId(UUID.randomUUID().toString())
                                                .setConnectorId(connectorId)
                                                .build())
                                .setCreated(created)
                                .build();
        }

        private String buildS3Key(String connectorId, String docId) {
                // Standard path: connectors/{connectorId}/{docId}.pb
                return String.format("connectors/%s/%s.pb", connectorId, docId);
        }
}
