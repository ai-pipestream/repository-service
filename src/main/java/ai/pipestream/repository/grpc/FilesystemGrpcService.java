package ai.pipestream.repository.grpc;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.filesystem.v1.GetFilesystemNodeRequest;
import ai.pipestream.repository.filesystem.v1.GetFilesystemNodeResponse;
import ai.pipestream.repository.filesystem.v1.MutinyFilesystemServiceGrpc;
import ai.pipestream.repository.filesystem.v1.Node;
import ai.pipestream.repository.service.DocumentStorageService;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.protobuf.util.Timestamps.fromMillis;

/**
 * Backward-compatible filesystem service implementation.
 * Supports minimum set needed for downstream consumers such as opensearch-manager
 * while phase migration to richer filesystem APIs is ongoing.
 */
@GrpcService
public class FilesystemGrpcService extends MutinyFilesystemServiceGrpc.FilesystemServiceImplBase {

    @Inject
    DocumentStorageService storageService;

    @Override
    @WithSession
    public Uni<GetFilesystemNodeResponse> getFilesystemNode(GetFilesystemNodeRequest request) {
        if (request == null || request.getDocumentId() == null || request.getDocumentId().isBlank()) {
            return Uni.createFrom().failure(
                    Status.INVALID_ARGUMENT.withDescription("document_id is required").asRuntimeException());
        }

        String accountId = request.getDrive();
        String documentId = request.getDocumentId();

        return storageService.findDocumentById(documentId)
                .map(records -> selectLatest(records, accountId))
                .map(optionalRecord -> optionalRecord.orElseThrow(() ->
                        Status.NOT_FOUND.withDescription("Document not found: " + documentId).asRuntimeException()))
                .map(this::toNode)
                .map(node -> GetFilesystemNodeResponse.newBuilder().setNode(node).build());
    }

    private Optional<PipeDocRecord> selectLatest(List<PipeDocRecord> records, String accountId) {
        if (records == null || records.isEmpty()) {
            return Optional.empty();
        }

        return records.stream()
                .filter(record -> accountId == null || accountId.isBlank() || accountId.equals(record.accountId))
                .sorted(Comparator.comparing((PipeDocRecord r) -> r.createdAt != null ? r.createdAt : Instant.EPOCH).reversed())
                .findFirst()
                .or(() -> records.stream()
                        .sorted(Comparator.comparing((PipeDocRecord r) -> r.createdAt != null ? r.createdAt : Instant.EPOCH).reversed())
                        .findFirst());
    }

    private Node toNode(PipeDocRecord record) {
        Node.Builder nodeBuilder = Node.newBuilder()
                .setDocumentId(record.docId == null ? "" : record.docId)
                .setName(record.filename == null ? "" : record.filename)
                .setPath(record.objectKey == null ? "" : record.objectKey)
                .setContentType(record.contentType == null ? "" : record.contentType)
                .setSizeBytes(record.sizeBytes == null ? 0L : record.sizeBytes)
                .setS3Key(record.pipedocObjectKey == null ? "" : record.pipedocObjectKey)
                .setType(Node.NodeType.NODE_TYPE_FILE);

        if (record.createdAt != null) {
            nodeBuilder.setCreatedAt(fromMillis(record.createdAt.toEpochMilli()));
        }

        return nodeBuilder.build();
    }

}
