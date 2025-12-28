package ai.pipestream.repository.graph;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for validating graph location IDs against the pipeline graph service.
 * <p>
 * This service ensures that graph_location_id values provided in SavePipeDocRequest
 * are valid node IDs that exist in any active pipeline graph.
 * This prevents invalid node IDs from being used (topology correctness validation).
 * <p>
 * Note: Graphs are owned by the platform/app, not by accounts. Accounts are data owners
 * (tied to datasources), not graph owners. Multiple accounts' data can traverse through
 * the same graph nodes. Therefore, validation only checks if the node ID exists in any
 * active graph, without account-based filtering.
 * <p>
 * TODO: Implement actual validation by calling the engine's PipelineGraphService
 * to check if the graph_location_id exists as a node ID in any active graph.
 */
@ApplicationScoped
public class GraphValidationService {

    private static final Logger LOG = Logger.getLogger(GraphValidationService.class);

    /**
     * Validates that a graph_location_id exists as a node ID in any active pipeline graph.
     * <p>
     * This ensures topology correctness by preventing invalid/made-up node IDs from being used.
     * No account-based validation is performed, as graphs are platform-owned infrastructure.
     *
     * @param graphLocationId The graph location ID (node ID) to validate
     * @return A Uni that completes with true if valid, false otherwise
     */
    public Uni<Boolean> validateGraphLocation(String graphLocationId) {
        LOG.debugf("Validating graph_location_id=%s", graphLocationId);
        
        // TODO: Implement actual validation by calling engine's PipelineGraphService
        // 1. Get all active graphs (or query GraphCache if it has a node ID index)
        // 2. Check if graphLocationId exists as a node ID in any of those graphs
        // 3. Return true if found, false otherwise
        
        // For now, log a warning and allow it (to be implemented properly)
        LOG.warnf("Graph validation not yet implemented - allowing graph_location_id=%s", 
                graphLocationId);
        return Uni.createFrom().item(true);
    }
}

