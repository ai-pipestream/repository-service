package ai.pipestream.repository.grpc;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.pipedoc.v1.*;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.service.MetadataService;
import ai.pipestream.repository.service.VersionControlService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.s3.S3Config;

/**
 * gRPC service implementation for Repository Service.
 * Provides remote access to repository operations.
 */
@GrpcService
public class RepositoryGrpcService extends MutinyPipeDocServiceGrpc.PipeDocServiceImplBase {

    private static final Logger LOG = Logger.getLogger(RepositoryGrpcService.class);

    @Inject
    DocumentStorageService storageService;

    @Inject
    VersionControlService versionService;

    @Inject
    MetadataService metadataService;

    @Inject
    S3AsyncClient s3AsyncClient;

    @Inject
    S3Config s3Config;

    public RepositoryGrpcService() {
        LOG.info("RepositoryGrpcService initialized");
    }

    @Override
    public Uni<SavePipeDocResponse> savePipeDoc(SavePipeDocRequest request) {
        LOG.debugf("savePipeDoc request: drive=%s, connector_id=%s, has_use_datasource_id=%s, has_graph_location_id=%s", 
                request.getDrive(), request.getConnectorId(), 
                request.hasUseDatasourceId(), request.hasGraphLocationId());
        
        if (!request.hasPipedoc()) {
            return Uni.createFrom().failure(new IllegalArgumentException("PipeDoc is required"));
        }
        if (request.getDrive() == null || request.getDrive().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Drive is required"));
        }

        PipeDoc docToSave = request.getPipedoc();
        
        // Extract graph_address from oneof: either use_datasource_id or graph_location_id
        String graphLocationId = null;
        if (request.hasGraphLocationId()) {
            graphLocationId = request.getGraphLocationId();
            LOG.debugf("Using graph_location_id: %s", graphLocationId);
        } else if (request.hasUseDatasourceId()) {
            // use_datasource_id is set - DocumentStorageService will use datasource_id from ownership
            LOG.debugf("Using datasource_id from PipeDoc ownership");
        } else {
            // Neither set - default to using datasource_id from ownership
            LOG.debugf("No graph_address specified, defaulting to datasource_id from PipeDoc ownership");
        }
        
        // Extract cluster_id (optional - null for intake, set for cluster processing)
        String clusterId = request.hasClusterId() ? request.getClusterId() : null;
        LOG.debugf("Using cluster_id: %s", clusterId != null ? clusterId : "null (intake)");
        
        // Use DocumentStorageService to store the document
        return storageService.store(docToSave, null, graphLocationId, clusterId)
                .map(stored -> SavePipeDocResponse.newBuilder()
                        .setNodeId(stored.documentId())
                        .setDrive(request.getDrive())
                        .setS3Key(stored.s3Key())
                        .setSizeBytes(stored.sizeBytes())
                        .setChecksum(stored.checksum())
                        .setCreatedAtEpochMs(System.currentTimeMillis()) // TODO: Use actual creation time from record
                        .build())
                .onFailure().invoke(throwable -> LOG.errorf(throwable, "Failed to save PipeDoc"));
    }

    @Override
    public Uni<GetPipeDocResponse> getPipeDoc(GetPipeDocRequest request) {
        LOG.debugf("getPipeDoc request: node_id=%s", request.getNodeId());
        
        if (request.getNodeId() == null || request.getNodeId().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Node ID is required"));
        }

        String docId = request.getNodeId();
        
        // Use DocumentStorageService to retrieve the document
        return storageService.get(docId)
                .flatMap(doc -> {
                    if (doc == null) {
                        return Uni.createFrom().failure(new RuntimeException("Document not found: " + docId));
                    }
                    
                    // Fetch metadata for additional response fields
                    return PipeDocRecord.<PipeDocRecord>find("docId", docId).firstResult()
                            .map(record -> {
                                if (record == null) {
                                    // Fallback if record not found (shouldn't happen)
                                    // Fallback if record not found (shouldn't happen)
                                    // This case should have been handled above, but keeping for safety
                                    return GetPipeDocResponse.newBuilder()
                                            .setPipedoc(doc)
                                            .setNodeId(request.getNodeId()) // Use the requested node_id as-is
                                            .setDrive("default")
                                            .setSizeBytes(0)
                                            .setRetrievedAtEpochMs(System.currentTimeMillis())
                                            .build();
                                }
                                
                                return GetPipeDocResponse.newBuilder()
                                        .setPipedoc(doc)
                                        .setNodeId(record.nodeId.toString()) // Use UUID node_id
                                        .setDrive(record.driveName)
                                        .setSizeBytes(record.sizeBytes != null ? record.sizeBytes : 0)
                                        .setRetrievedAtEpochMs(System.currentTimeMillis())
                                        .build();
                            });
                })
                .onFailure().invoke(throwable -> LOG.errorf(throwable, "Failed to get PipeDoc: node_id=%s", request.getNodeId()));
    }

    @Override
    public Uni<GetPipeDocByReferenceResponse> getPipeDocByReference(GetPipeDocByReferenceRequest request) {
        if (!request.hasDocumentRef()) {
            return Uni.createFrom().failure(new IllegalArgumentException("DocumentReference is required"));
        }
        
        DocumentReference ref = request.getDocumentRef();
        LOG.debugf("getPipeDocByReference request: doc_id=%s, source_node_id=%s, account_id=%s", 
                ref.getDocId(), ref.getSourceNodeId(), ref.getAccountId());
        
        String docId = ref.getDocId();
        String graphAddressId = ref.getSourceNodeId(); // Note: proto field still named source_node_id, represents graph_address_id
        String accountId = ref.getAccountId();
        
        if (docId == null || docId.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("DocumentReference.doc_id is required"));
        }
        if (graphAddressId == null || graphAddressId.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("DocumentReference.source_node_id (graph_address_id) is required"));
        }
        if (accountId == null || accountId.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("DocumentReference.account_id is required"));
        }
        
        // Look up the document record by composite key (doc_id, graph_address_id, account_id)
        return PipeDocRecord.<PipeDocRecord>find("docId = ?1 and graphAddressId = ?2 and accountId = ?3", 
                docId, graphAddressId, accountId).firstResult()
                .flatMap(record -> {
                    if (record == null) {
                        LOG.warnf("Document not found: doc_id=%s, account_id=%s", docId, accountId);
                        return Uni.createFrom().failure(new RuntimeException(
                                String.format("Document not found: doc_id=%s, account_id=%s", docId, accountId)));
                    }
                    
                    // Fetch the PipeDoc from S3
                    return Uni.createFrom().completionStage(
                            s3AsyncClient.getObject(
                                    GetObjectRequest.builder()
                                            .bucket(s3Config.bucket())
                                            .key(record.pipedocObjectKey)
                                            .build(),
                                    AsyncResponseTransformer.toBytes()
                            )
                    )
                    .map(response -> {
                        try {
                            PipeDoc doc = PipeDoc.parseFrom(response.asByteArray());
                            
                            return GetPipeDocByReferenceResponse.newBuilder()
                                    .setPipedoc(doc)
                                    .setNodeId(record.nodeId.toString()) // Use UUID node_id
                                    .setDrive(record.driveName)
                                    .setSizeBytes(record.sizeBytes != null ? record.sizeBytes : 0)
                                    .setRetrievedAtEpochMs(System.currentTimeMillis())
                                    .build();
                        } catch (Exception e) {
                            LOG.errorf(e, "Failed to parse PipeDoc from S3: doc_id=%s", docId);
                            throw new RuntimeException("Failed to parse PipeDoc", e);
                        }
                    })
                    .onFailure(NoSuchKeyException.class).recoverWithUni(throwable -> {
                        LOG.errorf(throwable, "S3 object not found: key=%s, doc_id=%s", record.pipedocObjectKey, docId);
                        return Uni.createFrom().failure(new RuntimeException("Document data not found in storage"));
                    });
                })
                .onFailure().invoke(throwable -> LOG.errorf(throwable, 
                        "Failed to get PipeDoc by reference: doc_id=%s, account_id=%s", docId, accountId));
    }

    @Override
    public Uni<ListPipeDocsResponse> listPipeDocs(ListPipeDocsRequest request) {
        // TODO: Implement listPipeDocs
        LOG.warnf("listPipeDocs not yet implemented");
        return Uni.createFrom().failure(new UnsupportedOperationException("listPipeDocs not yet implemented"));
    }

    @Override
    public Uni<GetBlobResponse> getBlob(GetBlobRequest request) {
        if (!request.hasStorageRef()) {
            return Uni.createFrom().failure(new IllegalArgumentException("FileStorageReference is required"));
        }

        ai.pipestream.data.v1.FileStorageReference storageRef = request.getStorageRef();
        String objectKey = storageRef.getObjectKey();
        String driveName = storageRef.getDriveName();

        if (objectKey == null || objectKey.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("FileStorageReference.object_key is required"));
        }
        if (driveName == null || driveName.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("FileStorageReference.drive_name is required"));
        }

        LOG.debugf("Fetching blob from S3: drive=%s, object_key=%s", driveName, objectKey);

        // Build S3 GetObjectRequest - include version_id if provided
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(s3Config.bucket())
                .key(objectKey);

        if (storageRef.hasVersionId() && !storageRef.getVersionId().isEmpty()) {
            requestBuilder.versionId(storageRef.getVersionId());
        }

        // Fetch blob bytes from S3 (matches pattern from GetPipeDocByReference - no emitOn, direct S3 access)
        return Uni.createFrom().completionStage(
                s3AsyncClient.getObject(requestBuilder.build(), AsyncResponseTransformer.toBytes())
        )
        .map(response -> {
            byte[] blobData = response.asByteArray();
            long sizeBytes = blobData.length;
            // Access content type from the underlying GetObjectResponse
            String contentType = response.response().contentType();

            LOG.debugf("Retrieved blob from S3: object_key=%s, size_bytes=%d, content_type=%s", 
                    objectKey, sizeBytes, contentType);

            GetBlobResponse.Builder responseBuilder = GetBlobResponse.newBuilder()
                    .setData(com.google.protobuf.ByteString.copyFrom(blobData))
                    .setSizeBytes(sizeBytes)
                    .setRetrievedAtEpochMs(System.currentTimeMillis());

            if (contentType != null && !contentType.isEmpty()) {
                responseBuilder.setMimeType(contentType);
            }

            return responseBuilder.build();
        })
        .onFailure(NoSuchKeyException.class).recoverWithUni(throwable -> {
            LOG.errorf(throwable, "S3 blob not found: object_key=%s, drive=%s", objectKey, driveName);
            return Uni.createFrom().failure(new RuntimeException("Blob not found in storage"));
        })
        .onFailure().invoke(throwable -> LOG.errorf(throwable, 
                "Failed to get blob: object_key=%s, drive=%s", objectKey, driveName));
    }
}
