package ai.pipestream.repository.grpc;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.http.MinioTestResource;
import ai.pipestream.repository.pipedoc.v1.*;
import ai.pipestream.repository.util.PipeDocUuidGenerator;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RepositoryGrpcService.
 * Tests the SavePipeDoc RPC with the new oneof graph_address structure.
 * Uses real PostgreSQL (via Dev Services), MinIO, and WireMock for Account Service.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource.class)
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class RepositoryGrpcServiceTest {

    @GrpcClient("repository-service")
    PipeDocServiceGrpc.PipeDocServiceBlockingStub pipeDocService;

    @Inject
    PipeDocUuidGenerator uuidGenerator;

    private PipeDoc createTestDoc(String docId, String accountId, String datasourceId, String connectorId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .setConnectorId(connectorId)
                        .build())
                .build();
    }

    @Test
    void savePipeDocWithUseDatasourceIdUsesDatasourceIdAsGraphAddress() {
        String docId = UUID.randomUUID().toString();
        String accountId = "valid-account";
        String datasourceId = "test-datasource-123";
        String connectorId = "test-connector";
        PipeDoc doc = createTestDoc(docId, accountId, datasourceId, connectorId);

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive("default")
                .setUseDatasourceId(true)  // Use oneof: use_datasource_id
                .build();

        SavePipeDocResponse response = pipeDocService.savePipeDoc(request);

        assertNotNull(response);
        assertNotNull(response.getNodeId());
        assertFalse(response.getNodeId().isBlank());
        assertEquals("default", response.getDrive());
        
        // Verify the node_id matches expected UUID (deterministic based on doc_id, datasource_id, account_id)
        UUID expectedNodeId = uuidGenerator.generateNodeId(docId, datasourceId, accountId);
        assertEquals(expectedNodeId.toString(), response.getNodeId());
    }

    @Test
    void savePipeDocWithGraphLocationIdUsesProvidedLocation() {
        String docId = UUID.randomUUID().toString();
        String accountId = "valid-account";
        String datasourceId = "test-datasource-456";
        String connectorId = "test-connector";
        String graphLocationId = "graph-node-processor-1";
        PipeDoc doc = createTestDoc(docId, accountId, datasourceId, connectorId);

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive("default")
                .setGraphLocationId(graphLocationId)  // Use oneof: graph_location_id
                .build();

        SavePipeDocResponse response = pipeDocService.savePipeDoc(request);

        assertNotNull(response);
        assertNotNull(response.getNodeId());
        assertFalse(response.getNodeId().isBlank());
        assertEquals("default", response.getDrive());
        
        // Verify the node_id matches expected UUID (deterministic based on doc_id, graph_location_id, account_id)
        UUID expectedNodeId = uuidGenerator.generateNodeId(docId, graphLocationId, accountId);
        assertEquals(expectedNodeId.toString(), response.getNodeId());
    }

    @Test
    void savePipeDocWithoutOneofDefaultsToDatasourceId() {
        String docId = UUID.randomUUID().toString();
        String accountId = "valid-account";
        String datasourceId = "test-datasource-default";
        String connectorId = "test-connector";
        PipeDoc doc = createTestDoc(docId, accountId, datasourceId, connectorId);

        // Create request without setting either oneof field (neither use_datasource_id nor graph_location_id)
        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive("default")
                .build();

        SavePipeDocResponse response = pipeDocService.savePipeDoc(request);

        assertNotNull(response);
        assertNotNull(response.getNodeId());
        
        // Should default to using datasource_id as graph_address_id
        UUID expectedNodeId = uuidGenerator.generateNodeId(docId, datasourceId, accountId);
        assertEquals(expectedNodeId.toString(), response.getNodeId());
    }

    @Test
    void savePipeDocWithSameDocIdButDifferentGraphAddressCreatesDifferentRecords() {
        String docId = UUID.randomUUID().toString();
        String accountId = "valid-account";
        String datasourceId = "test-datasource-multi";
        String connectorId = "test-connector";
        
        // First save: initial intake (datasource_id)
        PipeDoc doc1 = createTestDoc(docId, accountId, datasourceId, connectorId);
        SavePipeDocRequest request1 = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc1)
                .setDrive("default")
                .setUseDatasourceId(true)
                .build();
        SavePipeDocResponse response1 = pipeDocService.savePipeDoc(request1);
        UUID nodeId1 = UUID.fromString(response1.getNodeId());

        // Second save: after processing at graph node (different graph_address_id)
        PipeDoc doc2 = createTestDoc(docId, accountId, datasourceId, connectorId);
        String graphLocationId = "graph-node-processor-2";
        SavePipeDocRequest request2 = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc2)
                .setDrive("default")
                .setGraphLocationId(graphLocationId)
                .build();
        SavePipeDocResponse response2 = pipeDocService.savePipeDoc(request2);
        UUID nodeId2 = UUID.fromString(response2.getNodeId());

        // Verify both records have different node_ids (different UUIDs)
        assertNotEquals(nodeId1, nodeId2, "Different graph_address_id values should create different node_ids");
        
        // Verify node_ids match expected deterministic UUIDs
        UUID expectedNodeId1 = uuidGenerator.generateNodeId(docId, datasourceId, accountId);
        UUID expectedNodeId2 = uuidGenerator.generateNodeId(docId, graphLocationId, accountId);
        assertEquals(expectedNodeId1, nodeId1);
        assertEquals(expectedNodeId2, nodeId2);
    }

    @Test
    void savePipeDocWithInvalidPipeDocFails() {
        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setDrive("default")
                .setUseDatasourceId(true)
                .build();

        // Should fail because pipedoc is required
        assertThrows(Exception.class, () -> pipeDocService.savePipeDoc(request));
    }

    @Test
    void savePipeDocWithInvalidAccountFails() {
        String docId = UUID.randomUUID().toString();
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("nonexistent") // Invalid account
                        .setDatasourceId("test-datasource")
                        .setConnectorId("test-connector")
                        .build())
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive("default")
                .setUseDatasourceId(true)
                .build();

        // Should fail because account doesn't exist
        assertThrows(Exception.class, () -> pipeDocService.savePipeDoc(request));
    }
}

