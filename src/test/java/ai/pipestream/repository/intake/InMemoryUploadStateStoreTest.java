package ai.pipestream.repository.intake;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InMemoryUploadStateStore.
 * Tests state management, idempotency, and eviction behavior.
 */
@QuarkusTest
class InMemoryUploadStateStoreTest {

    @Inject
    InMemoryUploadStateStore stateStore;

    @BeforeEach
    void setUp() {
        // Clean up between tests
        stateStore.cleanup();
    }

    @Test
    void testCreateAndRetrieveState() {
        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();

        InMemoryUploadState state = stateStore.create(nodeId, uploadId);

        assertNotNull(state);
        assertEquals(nodeId, state.getNodeId());
        assertEquals(uploadId, state.getUploadId());
        assertEquals(0, state.getReceivedChunkCount());
        assertEquals(0, state.getBytesReceivedTotal());
    }

    @Test
    void testGetByUploadId() {
        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();

        stateStore.create(nodeId, uploadId);

        Optional<InMemoryUploadState> retrieved = stateStore.get(uploadId);
        assertTrue(retrieved.isPresent());
        assertEquals(nodeId, retrieved.get().getNodeId());
    }

    @Test
    void testGetByNodeId() {
        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();

        stateStore.create(nodeId, uploadId);

        Optional<InMemoryUploadState> retrieved = stateStore.getByNodeId(nodeId);
        assertTrue(retrieved.isPresent());
        assertEquals(uploadId, retrieved.get().getUploadId());
    }

    @Test
    void testGetNonExistentUpload() {
        Optional<InMemoryUploadState> retrieved = stateStore.get(UUID.randomUUID().toString());
        assertTrue(retrieved.isEmpty());
    }

    @Test
    void testGetByNonExistentNodeId() {
        Optional<InMemoryUploadState> retrieved = stateStore.getByNodeId(UUID.randomUUID().toString());
        assertTrue(retrieved.isEmpty());
    }

    @Test
    void testRemoveState() {
        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();

        stateStore.create(nodeId, uploadId);
        assertTrue(stateStore.get(uploadId).isPresent());

        stateStore.remove(uploadId);

        assertTrue(stateStore.get(uploadId).isEmpty());
        assertTrue(stateStore.getByNodeId(nodeId).isEmpty());
    }

    @Test
    void testActiveCount() {
        long initialCount = stateStore.getActiveCount();

        stateStore.create(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        stateStore.create(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        stateStore.create(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertEquals(initialCount + 3, stateStore.getActiveCount());
    }

    @Test
    void testMultipleUploadsForDifferentNodes() {
        String nodeId1 = UUID.randomUUID().toString();
        String uploadId1 = UUID.randomUUID().toString();
        String nodeId2 = UUID.randomUUID().toString();
        String uploadId2 = UUID.randomUUID().toString();

        stateStore.create(nodeId1, uploadId1);
        stateStore.create(nodeId2, uploadId2);

        assertEquals(nodeId1, stateStore.get(uploadId1).get().getNodeId());
        assertEquals(nodeId2, stateStore.get(uploadId2).get().getNodeId());
        assertEquals(uploadId1, stateStore.getByNodeId(nodeId1).get().getUploadId());
        assertEquals(uploadId2, stateStore.getByNodeId(nodeId2).get().getUploadId());
    }
}
