package ai.pipestream.repository.graph;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for validating graph location IDs against the pipeline graph service.
 * <p>
 * This service ensures that graph_location_id values provided in SavePipeDocRequest
 * are valid node IDs that exist in the account's active pipeline graphs.
 * This prevents masking attacks by ensuring only legitimate graph nodes can be used.
 * <p>
 * TODO: Implement actual validation by calling the engine's PipelineGraphService
 * to check if the graph_location_id exists in any active graph for the account.
 */
@ApplicationScoped
public class GraphValidationService {

    private static final Logger LOG = Logger.getLogger(GraphValidationService.class);

    /**
     * Validates that a graph_location_id exists in the account's active pipeline graphs.
     * <p>
     * This prevents masking attacks by ensuring only legitimate graph nodes can be used
     * as graph_location_id values.
     *
     * @param graphLocationId The graph location ID to validate
     * @param accountId The account ID that should own the graph containing this location
     * @return A Uni that completes with true if valid, false otherwise
     */
    public Uni<Boolean> validateGraphLocation(String graphLocationId, String accountId) {
        LOG.debugf("Validating graph_location_id=%s for account_id=%s", graphLocationId, accountId);
        
        // TODO: Implement actual validation by calling engine's PipelineGraphService
        // 1. Get all active graphs for the account
        // 2. Check if graphLocationId exists as a node ID in any of those graphs
        // 3. Return true if found, false otherwise
        
        // For now, log a warning and allow it (to be implemented properly)
        LOG.warnf("Graph validation not yet implemented - allowing graph_location_id=%s for account_id=%s", 
                graphLocationId, accountId);
        return Uni.createFrom().item(true);
    }
}

