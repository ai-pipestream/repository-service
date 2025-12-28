package ai.pipestream.repository.util;

import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Generates deterministic UUIDs for PipeDoc repository node identifiers.
 * <p>
 * Uses a composite key of (doc_id, graph_address_id, account_id) to generate
 * a deterministic UUID via name-based hashing (UUID v5 equivalent).
 * This ensures the same document at the same graph location per account always
 * gets the same UUID, which is useful for:
 * <ul>
 *   <li>Kafka key generation (consistent partitioning)</li>
 *   <li>Idempotent operations (same inputs = same UUID)</li>
 *   <li>Primary key consistency in the database</li>
 * </ul>
 */
@ApplicationScoped
public class PipeDocUuidGenerator {

    /** Separator used in composite key to prevent collisions. */
    private static final String SEPARATOR = "|";

    /**
     * Generates a deterministic UUID for a repository node identifier.
     * <p>
     * The UUID is deterministically derived from the three logical identifiers:
     * - doc_id: The document identifier
     * - graphAddressId: The graph address ID (can be datasource_id for intake or graph node ID after processing)
     * - account_id: The account identifier (for multi-tenancy)
     *
     * @param docId The document identifier
     * @param graphAddressId The graph address ID (datasource_id or graph node ID)
     * @param accountId The account identifier
     * @return A deterministic UUID generated from the composite key
     */
    public UUID generateNodeId(String docId, String graphAddressId, String accountId) {
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException("docId cannot be null or blank");
        }
        if (graphAddressId == null || graphAddressId.isBlank()) {
            throw new IllegalArgumentException("graphAddressId cannot be null or blank");
        }
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("accountId cannot be null or blank");
        }

        String composite = docId + SEPARATOR + graphAddressId + SEPARATOR + accountId;
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8));
    }
}

