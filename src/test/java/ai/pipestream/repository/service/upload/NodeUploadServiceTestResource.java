package ai.pipestream.repository.service.upload;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Test resource that configures the node-upload-service gRPC client to connect to the gRPC server.
 * 
 * When use-separate-server=true, the gRPC server runs on a different port than HTTP.
 * We use a fixed test port (9001) so the client can be configured before Quarkus starts.
 */
public class NodeUploadServiceTestResource implements QuarkusTestResourceLifecycleManager {
    
    private static final Logger LOG = Logger.getLogger(NodeUploadServiceTestResource.class);
    
    // Fixed port for gRPC server in tests (matches quarkus.grpc.server.test-port)
    private static final int GRPC_TEST_PORT = 9002;

    @Override
    public Map<String, String> start() {
        LOG.infof("NodeUploadServiceTestResource: Configuring node-upload-service client to connect to gRPC server on localhost:%d (use-separate-server=true)", GRPC_TEST_PORT);
        
        // Configure client to connect to the fixed gRPC test port
        return Map.of(
            "quarkus.grpc.clients.node-upload-service.host", "localhost",
            "quarkus.grpc.clients.node-upload-service.port", String.valueOf(GRPC_TEST_PORT)
        );
    }

    @Override
    public void stop() {
        // No cleanup needed
    }
}

