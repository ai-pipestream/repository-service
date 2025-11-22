package io.pipeline.repository.services;

import com.google.protobuf.Any;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.repository.filesystem.*;
import io.pipeline.repository.pipedoc.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@GrpcService
public class PipeDocServiceImpl extends MutinyPipeDocServiceGrpc.PipeDocServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(PipeDocServiceImpl.class);
    private static final String PIPEDOC_MIME_TYPE = "application/x-protobuf-pipedoc";
    private static final String PIPEDOC_SERVICE_TYPE = "PipeDocService";
    private static final String PIPEDOC_PAYLOAD_TYPE = "PipeDoc";

    @GrpcClient("filesystem-service")
    MutinyFilesystemServiceGrpc.MutinyFilesystemServiceStub filesystemService;

    @Override
    public Uni<SavePipeDocResponse> savePipeDoc(SavePipeDocRequest request) {
        if (request.getPipedoc() == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("PipeDoc is required"));
        }
        if (request.getDrive() == null || request.getDrive().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Drive is required"));
        }
        if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Connector ID is required"));
        }

        // Build metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("doc_id", request.getPipedoc().getDocId());
        metadata.put("connector_id", request.getConnectorId());

        if (request.getPipedoc().hasSearchMetadata()) {
            if (request.getPipedoc().getSearchMetadata().hasTitle()) {
                metadata.put("title", request.getPipedoc().getSearchMetadata().getTitle());
            }
            if (request.getPipedoc().getSearchMetadata().hasDocumentType()) {
                metadata.put("document_type", request.getPipedoc().getSearchMetadata().getDocumentType());
            }
            if (request.getPipedoc().getSearchMetadata().hasSourceUri()) {
                metadata.put("source_uri", request.getPipedoc().getSearchMetadata().getSourceUri());
            }
        }

        // Add custom metadata from request
        metadata.putAll(request.getMetadataMap());

        // Convert metadata to JSON string
        String metadataJson;
        try {
            metadataJson = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(metadata);
        } catch (Exception e) {
            log.warn("Failed to serialize metadata", e);
            metadataJson = "{}";
        }

        // Create node using FilesystemService
        CreateNodeRequest createRequest = CreateNodeRequest.newBuilder()
            .setDrive(request.getDrive())
            .setDocumentId(request.getPipedoc().getDocId())
            .setConnectorId(request.getConnectorId())
            .setName("pipedoc-" + request.getPipedoc().getDocId())
            .setContentType(PIPEDOC_MIME_TYPE)
            .setPayload(Any.pack(request.getPipedoc()))
            .setMetadata(metadataJson)
            .setType(Node.NodeType.FILE)
            .setServiceType(PIPEDOC_SERVICE_TYPE)
            .setPayloadType(PIPEDOC_PAYLOAD_TYPE)
            .setNodeTypeId(1) // FILE type
            .build();

        return filesystemService.createNode(createRequest)
            .map(node -> SavePipeDocResponse.newBuilder()
                .setNodeId(node.getDocumentId())
                .setDrive(request.getDrive())
                .setS3Key(node.getS3Key())
                .setSizeBytes(node.getSizeBytes())
                .setChecksum("") // FilesystemService doesn't expose checksum
                .setCreatedAtEpochMs(node.getCreatedAt().getSeconds() * 1000)
                .build());
    }

    @Override
    public Uni<GetPipeDocResponse> getPipeDoc(GetPipeDocRequest request) {
        if (request.getNodeId() == null || request.getNodeId().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Node ID is required"));
        }

        // Use FilesystemService to get the node with payload
        GetNodeRequest getRequest = GetNodeRequest.newBuilder()
            .setDocumentId(request.getNodeId())
            .setIncludePayload(true)
            .build();

        // Try default drive first, then search all drives
        return tryGetNodeFromDefaultDrive(getRequest)
            .onFailure().recoverWithUni(throwable -> searchAllDrivesForNode(request.getNodeId()))
            .map(node -> {
                try {
                    // Unpack the PipeDoc from the payload
                    PipeDoc pipeDoc = node.getPayload().unpack(PipeDoc.class);

                    return GetPipeDocResponse.newBuilder()
                        .setPipedoc(pipeDoc)
                        .setNodeId(node.getDocumentId())
                        .setDrive(extractDriveFromNode(node))
                        .setSizeBytes(node.getSizeBytes())
                        .setRetrievedAtEpochMs(System.currentTimeMillis())
                        .build();
                } catch (Exception e) {
                    log.error("Failed to unpack PipeDoc from node payload", e);
                    throw new RuntimeException("Failed to unpack PipeDoc", e);
                }
            });
    }

    @Override
    public Uni<ListPipeDocsResponse> listPipeDocs(ListPipeDocsRequest request) {
        // Use SearchNodes to find all PipeDocs
        SearchNodesRequest.Builder searchBuilder = SearchNodesRequest.newBuilder()
            .setQuery("content_type:" + PIPEDOC_MIME_TYPE);

        if (request.getDrive() != null && !request.getDrive().isEmpty()) {
            searchBuilder.setDrive(request.getDrive());
        }

        if (request.getLimit() > 0) {
            searchBuilder.setPageSize(request.getLimit());
        } else {
            searchBuilder.setPageSize(100);
        }

        // Add metadata filter for connector_id if specified
        if (request.getConnectorId() != null && !request.getConnectorId().isEmpty()) {
            searchBuilder.putMetadataFilters("connector_id", request.getConnectorId());
        }

        return filesystemService.searchNodes(searchBuilder.build())
            .map(response -> {
                ListPipeDocsResponse.Builder listResponse = ListPipeDocsResponse.newBuilder();

                for (SearchResult result : response.getNodesList()) {
                    Node node = result.getNode();
                    PipeDocMetadata.Builder metadataBuilder = PipeDocMetadata.newBuilder()
                        .setNodeId(node.getDocumentId())
                        .setDrive(extractDriveFromNode(node))
                        .setSizeBytes(node.getSizeBytes())
                        .setCreatedAtEpochMs(node.getCreatedAt().getSeconds() * 1000);

                    // Parse metadata JSON to extract fields
                    if (node.getMetadata() != null && !node.getMetadata().isEmpty()) {
                        try {
                            Map<String, String> metadata = parseMetadata(node.getMetadata());
                            metadataBuilder.putAllMetadata(metadata);

                            if (metadata.containsKey("doc_id")) {
                                metadataBuilder.setDocId(metadata.get("doc_id"));
                            }
                            if (metadata.containsKey("connector_id")) {
                                metadataBuilder.setConnectorId(metadata.get("connector_id"));
                            }
                            if (metadata.containsKey("title")) {
                                metadataBuilder.setTitle(metadata.get("title"));
                            }
                            if (metadata.containsKey("document_type")) {
                                metadataBuilder.setDocumentType(metadata.get("document_type"));
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse metadata for node " + node.getDocumentId(), e);
                        }
                    }

                    listResponse.addPipedocs(metadataBuilder.build());
                }

                listResponse.setTotalCount(response.getTotalCount());
                if (response.getNextPageToken() != null && !response.getNextPageToken().isEmpty()) {
                    listResponse.setNextContinuationToken(response.getNextPageToken());
                }

                return listResponse.build();
            });
    }

    private Uni<Node> tryGetNodeFromDefaultDrive(GetNodeRequest request) {
        // Try with default drive first
        GetNodeRequest requestWithDrive = GetNodeRequest.newBuilder(request)
            .setDrive("test-drive") // TODO: make this configurable
            .build();

        return filesystemService.getNode(requestWithDrive);
    }

    private Uni<Node> searchAllDrivesForNode(String documentId) {
        // Search across all drives for the node
        SearchNodesRequest searchRequest = SearchNodesRequest.newBuilder()
            .setQuery("document_id:" + documentId)
            .setPageSize(1)
            .build();

        return filesystemService.searchNodes(searchRequest)
            .flatMap(response -> {
                if (response.getNodesCount() > 0) {
                    SearchResult result = response.getNodes(0);
                    Node node = result.getNode();
                    // Now get the full node with payload
                    GetNodeRequest getRequest = GetNodeRequest.newBuilder()
                        .setDrive(extractDriveFromNode(node))
                        .setDocumentId(documentId)
                        .setIncludePayload(true)
                        .build();
                    return filesystemService.getNode(getRequest);
                } else {
                    return Uni.createFrom().failure(new RuntimeException("Node not found: " + documentId));
                }
            });
    }

    private String extractDriveFromNode(Node node) {
        // Extract drive name from metadata or use default
        if (node.getMetadata() != null && !node.getMetadata().isEmpty()) {
            try {
                Map<String, String> metadata = parseMetadata(node.getMetadata());
                if (metadata.containsKey("drive")) {
                    return metadata.get("drive");
                }
            } catch (Exception e) {
                // Ignore
            }
        }
        return "test-drive"; // Default drive
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseMetadata(String metadataJson) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(metadataJson, Map.class);
        } catch (Exception e) {
            return new HashMap<>();
        }
    }
}