package ai.pipestream.repository.service.upload;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * gRPC service for handling node uploads following the intake-first pattern.
 * 
 * Phase 1: InitiateUpload - Returns node_id and upload_id immediately
 * Phase 2: UploadChunk - Accepts chunks in parallel (unary calls)
 * 
 * Design reference: docs/new-design/01-upload-flow.md
 */
@ApplicationScoped
public class NodeUploadService {

    private static final Logger LOG = Logger.getLogger(NodeUploadService.class);

    public NodeUploadService() {
        LOG.info("NodeUploadService initialized");
    }

    // TODO: Implement InitiateUpload gRPC endpoint
    // - Generate node_id (or use client-provided)
    // - Create MySQL node record (status=PENDING)
    // - Initiate S3 multipart upload
    // - Initialize upload session state (DB-backed)
    // - Return response immediately
    
    // TODO: Implement UploadChunk gRPC endpoint (unary)
    // - Validate upload_id exists
    // - Persist chunk/session state (DB-backed) or stream directly to object storage
    // - Update received_chunks counter
    // - Publish chunk_received event
    // - Return acknowledgment immediately
}
