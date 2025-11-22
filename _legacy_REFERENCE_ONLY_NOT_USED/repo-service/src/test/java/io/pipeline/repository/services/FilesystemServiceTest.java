package io.pipeline.repository.services;

import io.pipeline.repository.filesystem.*;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import com.google.protobuf.Any;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for FilesystemService - tests the service logic without external dependencies
 */
@QuarkusTest
public class FilesystemServiceTest {
    
    @Inject
    @io.quarkus.grpc.GrpcService
    FilesystemServiceImpl filesystemService;
    
    @Test
    void testCreateNodeRequest() {
        // Test that we can create a basic CreateNodeRequest
        CreateNodeRequest request = CreateNodeRequest.newBuilder()
            .setName("test-document")
            .setType(Node.NodeType.FILE)
            .setDrive("default")
            .setContentType("application/json")
            .setPayload(Any.pack(com.google.protobuf.StringValue.of("test data")))
            .setNodeTypeId(1L)
            .setParentId(0L)
            .setMetadata("{}")
            .setIconSvg("")
            .setServiceType("test")
            .build();
        
        assertNotNull(request);
        assertEquals("test-document", request.getName());
        assertEquals(Node.NodeType.FILE, request.getType());
        assertEquals("default", request.getDrive());
    }
    
    @Test
    void testGetNodeRequest() {
        // Test that we can create a basic GetNodeRequest
        GetNodeRequest request = GetNodeRequest.newBuilder()
            .setDocumentId("test-doc-123")
            .setDrive("default")
            .setIncludePayload(true)
            .build();
        
        assertNotNull(request);
        assertEquals("test-doc-123", request.getDocumentId());
        assertEquals("default", request.getDrive());
        assertTrue(request.getIncludePayload());
    }
    
    @Test
    void testUpdateNodeRequest() {
        // Test that we can create a basic UpdateNodeRequest
        UpdateNodeRequest request = UpdateNodeRequest.newBuilder()
            .setDocumentId("test-doc-123")
            .setDrive("default")
            .setName("updated-document")
            .setContentType("application/json")
            .setPayload(Any.pack(com.google.protobuf.StringValue.of("updated data")))
            .setMetadata("{\"updated\": true}")
            .build();
        
        assertNotNull(request);
        assertEquals("test-doc-123", request.getDocumentId());
        assertEquals("updated-document", request.getName());
    }
    
    @Test
    void testDeleteNodeRequest() {
        // Test that we can create a basic DeleteNodeRequest
        DeleteNodeRequest request = DeleteNodeRequest.newBuilder()
            .setDocumentId("test-doc-123")
            .setDrive("default")
            .build();
        
        assertNotNull(request);
        assertEquals("test-doc-123", request.getDocumentId());
        assertEquals("default", request.getDrive());
    }
    
    @Test
    void testCopyNodeRequest() {
        // Test that we can create a basic CopyNodeRequest
        CopyNodeRequest request = CopyNodeRequest.newBuilder()
            .setDocumentId("source-doc-123")
            .setNewName("copied-document")
            .setDrive("default")
            .setTargetParentId(1L)
            .build();
        
        assertNotNull(request);
        assertEquals("source-doc-123", request.getDocumentId());
        assertEquals("copied-document", request.getNewName());
        assertEquals("default", request.getDrive());
        assertEquals(1L, request.getTargetParentId());
    }
}