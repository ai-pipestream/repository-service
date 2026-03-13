package ai.pipestream.repository.grpc;

import ai.pipestream.repository.entity.Document;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.v1.*;
import ai.pipestream.repository.service.DocumentStorageService;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.util.Timestamps.fromMillis;

/**
 * Implementation of the DocumentService gRPC service.
 * Provides logical document management and audit trail capabilities.
 */
@GrpcService
public class DocumentGrpcService extends MutinyDocumentServiceGrpc.DocumentServiceImplBase {

    private static final Logger LOG = Logger.getLogger(DocumentGrpcService.class);

    @Inject
    DocumentStorageService storageService;

    @Override
    @WithSession
    public Uni<SaveResponse> save(SaveRequest request) {
        if (!request.hasPipedoc()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT.withDescription("PipeDoc is required").asRuntimeException());
        }
        if (request.getDrive().isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT.withDescription("Drive is required").asRuntimeException());
        }

        String graphLocationId = request.getGraphLocationId();
        String clusterId = request.getClusterId();

        return storageService.store(request.getPipedoc(), null, graphLocationId, clusterId, request.getDrive())
                .map(stored -> SaveResponse.newBuilder()
                        .setNodeId(stored.documentId())
                        .setDocId(request.getPipedoc().getDocId())
                        .setS3Key(stored.s3Key())
                        .setSavedAt(fromMillis(stored.createdAtEpochMs()))
                        .build())
                .onFailure().invoke(e -> LOG.errorf(e, "Failed to save document: %s", request.getPipedoc().getDocId()));
    }

    @Override
    @WithSession
    public Uni<GetResponse> get(GetRequest request) {
        String docId = request.getDocId();
        String accountId = request.getAccountId();

        if (docId.isEmpty() || accountId.isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT.withDescription("doc_id and account_id are required").asRuntimeException());
        }

        return Document.<Document>find("documentId = ?1 and accountId = ?2", docId, accountId).firstResult()
                .map(doc -> {
                    if (doc == null) {
                        throw Status.NOT_FOUND.withDescription("Document not found: " + docId).asRuntimeException();
                    }
                    return toGetResponse(doc);
                });
    }

    @Override
    @WithSession
    public Uni<GetAuditTrailResponse> getAuditTrail(GetAuditTrailRequest request) {
        String docId = request.getDocId();
        String accountId = request.getAccountId();

        if (docId.isEmpty() || accountId.isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT.withDescription("doc_id and account_id are required").asRuntimeException());
        }

        return PipeDocRecord.<PipeDocRecord>find("docId = ?1 and accountId = ?2 order by createdAt asc", docId, accountId).list()
                .map(records -> {
                    GetAuditTrailResponse.Builder builder = GetAuditTrailResponse.newBuilder().setDocId(docId);
                    return builder.build();
                });
    }

    @Override
    @WithSession
    public Uni<ListResponse> list(ListRequest request) {
        int pageSize = request.getPageSize() > 0 ? Math.min(request.getPageSize(), 100) : 20;
        final int page = parsePageToken(request.getPageToken());

        String accountId = request.getAccountId();
        if (accountId.isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT.withDescription("account_id is required").asRuntimeException());
        }

        StringBuilder query = new StringBuilder("accountId = ?1");
        List<Object> params = new java.util.ArrayList<>();
        params.add(accountId);

        if (!request.getDatasourceId().isEmpty()) {
            query.append(" and datasourceId = ?").append(params.size() + 1);
            params.add(request.getDatasourceId());
        }

        return Document.<Document>find(query.toString() + " order by updatedAt desc", params.toArray())
                .page(page, pageSize).list()
                .map(docs -> {
                    ListResponse.Builder builder = ListResponse.newBuilder();
                    for (Document doc : docs) {
                        builder.addDocuments(toGetResponse(doc));
                    }
                    if (docs.size() == pageSize) {
                        builder.setNextPageToken(String.valueOf(page + 1));
                    }
                    return builder.build();
                });
    }

    private int parsePageToken(String pageToken) {
        if (pageToken == null || pageToken.isEmpty()) return 0;
        try {
            return Integer.parseInt(pageToken);
        } catch (NumberFormatException e) {
            LOG.warnf("Invalid page token: %s", pageToken);
            return 0;
        }
    }

    private GetResponse toGetResponse(Document doc) {
        return GetResponse.newBuilder()
                .setDocId(doc.documentId)
                .setTitle(doc.title)
                .setFilename(doc.filename)
                .setContentType(doc.contentType)
                .setSizeBytes(doc.contentSize)
                .setStatus(doc.status)
                .setAccountId(doc.accountId)
                .setDatasourceId(doc.datasourceId)
                .setCreatedAt(fromMillis(doc.createdAt.toEpochMilli()))
                .setUpdatedAt(fromMillis(doc.updatedAt.toEpochMilli()))
                .build();
    }
}
