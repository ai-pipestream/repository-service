package ai.pipestream.repository.service;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.pipestream.repository.service.upload.NodeUploadService;
import ai.pipestream.repository.service.state.UploadStateManager;
import ai.pipestream.repository.worker.ChunkProcessorWorker;
import ai.pipestream.repository.worker.UploadCompletionWorker;

import jakarta.inject.Inject;

/**
 * Basic test to verify skeleton services are properly configured and can be injected.
 */
@QuarkusTest
public class SkeletonServicesTest {

    @Inject
    NodeUploadService nodeUploadService;

    @Inject
    UploadStateManager uploadStateManager;

    @Inject
    ChunkProcessorWorker chunkProcessorWorker;

    @Inject
    UploadCompletionWorker uploadCompletionWorker;

    @Test
    public void testNodeUploadServiceInjection() {
        assertNotNull(nodeUploadService, "NodeUploadService should be injectable");
    }

    @Test
    public void testUploadStateManagerInjection() {
        assertNotNull(uploadStateManager, "UploadStateManager should be injectable");
    }

    @Test
    public void testChunkProcessorWorkerInjection() {
        assertNotNull(chunkProcessorWorker, "ChunkProcessorWorker should be injectable");
    }

    @Test
    public void testUploadCompletionWorkerInjection() {
        assertNotNull(uploadCompletionWorker, "UploadCompletionWorker should be injectable");
    }
}
