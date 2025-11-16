package ai.pipestream.repository.intake;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for InMemoryUploadState.
 * Tests idempotency, chunk tracking, and byte counting.
 */
class InMemoryUploadStateTest {

    @Test
    void testInitialState() {
        String nodeId = UUID.randomUUID().toString();
        String uploadId = UUID.randomUUID().toString();

        InMemoryUploadState state = new InMemoryUploadState(nodeId, uploadId);

        assertEquals(nodeId, state.getNodeId());
        assertEquals(uploadId, state.getUploadId());
        assertEquals(0, state.getReceivedChunkCount());
        assertEquals(0, state.getBytesReceivedTotal());
        assertNotNull(state.getCreatedAt());
        assertNotNull(state.getLastActivityTs());
    }

    @Test
    void testReceiveChunk() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        boolean added = state.receiveChunk(0, 1000);

        assertTrue(added);
        assertEquals(1, state.getReceivedChunkCount());
        assertEquals(1000, state.getBytesReceivedTotal());
    }

    @Test
    void testReceiveMultipleChunks() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        state.receiveChunk(0, 1000);
        state.receiveChunk(1, 2000);
        state.receiveChunk(2, 1500);

        assertEquals(3, state.getReceivedChunkCount());
        assertEquals(4500, state.getBytesReceivedTotal());
    }

    @Test
    void testIdempotencyDuplicateChunk() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        boolean firstAdd = state.receiveChunk(0, 1000);
        boolean secondAdd = state.receiveChunk(0, 1000);

        assertTrue(firstAdd);
        assertFalse(secondAdd); // Should not add again
        assertEquals(1, state.getReceivedChunkCount());
        assertEquals(1000, state.getBytesReceivedTotal()); // Bytes should not increase
    }

    @Test
    void testOutOfOrderChunks() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        state.receiveChunk(5, 1000);
        state.receiveChunk(2, 2000);
        state.receiveChunk(0, 1500);
        state.receiveChunk(10, 3000);

        assertEquals(4, state.getReceivedChunkCount());
        assertEquals(7500, state.getBytesReceivedTotal());
        assertTrue(state.hasChunk(5));
        assertTrue(state.hasChunk(2));
        assertTrue(state.hasChunk(0));
        assertTrue(state.hasChunk(10));
        assertFalse(state.hasChunk(1));
        assertFalse(state.hasChunk(3));
    }

    @Test
    void testHasChunk() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertFalse(state.hasChunk(0));

        state.receiveChunk(0, 1000);

        assertTrue(state.hasChunk(0));
        assertFalse(state.hasChunk(1));
    }

    @Test
    void testLastActivityTimestampUpdates() throws InterruptedException {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Instant initialTs = state.getLastActivityTs();

        Thread.sleep(10); // Small delay to ensure timestamp changes

        state.receiveChunk(0, 1000);

        assertTrue(state.getLastActivityTs().isAfter(initialTs));
    }

    @Test
    void testTouch() throws InterruptedException {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Instant initialTs = state.getLastActivityTs();

        Thread.sleep(10);

        state.touch();

        assertTrue(state.getLastActivityTs().isAfter(initialTs));
        assertEquals(0, state.getReceivedChunkCount()); // No chunks received
    }

    @Test
    void testLargeChunkNumbers() {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        state.receiveChunk(0, 1000);
        state.receiveChunk(Integer.MAX_VALUE - 1, 2000);
        state.receiveChunk(1000000, 3000);

        assertEquals(3, state.getReceivedChunkCount());
        assertTrue(state.hasChunk(Integer.MAX_VALUE - 1));
        assertTrue(state.hasChunk(1000000));
    }

    @Test
    void testConcurrentChunkReceipt() throws InterruptedException {
        InMemoryUploadState state = new InMemoryUploadState(
            UUID.randomUUID().toString(), UUID.randomUUID().toString());

        // Simulate concurrent chunk uploads
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            final int chunkNum = i;
            threads[i] = new Thread(() -> state.receiveChunk(chunkNum, 1000));
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(10, state.getReceivedChunkCount());
        assertEquals(10000, state.getBytesReceivedTotal());
    }
}
