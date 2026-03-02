package ai.pipestream.repository.service;

import ai.pipestream.repository.entity.Node;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Service for Node CRUD and tree operations.
 */
@ApplicationScoped
public class NodeService {

    private static final Logger LOG = Logger.getLogger(NodeService.class);

    /**
     * Create a node within a transaction. Computes path if not set.
     */
    public Uni<Node> createNode(Node node) {
        if (node.nodeId == null || node.nodeId.isBlank()) {
            node.nodeId = UUID.randomUUID().toString();
        }
        if (node.status == null || node.status.isBlank()) {
            node.status = "ACTIVE";
        }
        node.createdAt = Instant.now();
        node.updatedAt = Instant.now();

        Uni<Void> pathComputation;
        if (node.path == null || node.path.isBlank()) {
            pathComputation = computePath(node).invoke(p -> node.path = p).replaceWithVoid();
        } else {
            pathComputation = Uni.createFrom().voidItem();
        }

        return pathComputation.flatMap(v ->
                Panache.withTransaction(() -> node.<Node>persist())
        );
    }

    /**
     * Find a node by its nodeId (UUID string).
     */
    public Uni<Node> findByNodeId(String nodeId) {
        return Node.<Node>find("nodeId", nodeId).firstResult();
    }

    /**
     * Find a node by drive and path.
     */
    public Uni<Node> findByDriveAndPath(Long driveId, String path) {
        return Node.<Node>find("drive.id = ?1 AND path = ?2", driveId, path).firstResult();
    }

    /**
     * Get paginated children of a parent node within a drive.
     * parentId=null means root-level nodes.
     */
    public Uni<List<Node>> getChildren(Long driveId, Long parentId, int page, int pageSize,
                                        String orderBy, boolean ascending) {
        String sortField = (orderBy != null && !orderBy.isBlank()) ? orderBy : "name";
        String direction = ascending ? "ASC" : "DESC";
        String hql;
        Object[] params;

        if (parentId == null || parentId == 0) {
            hql = "drive.id = ?1 AND parentId IS NULL ORDER BY " + sortField + " " + direction;
            params = new Object[]{driveId};
        } else {
            hql = "drive.id = ?1 AND parentId = ?2 ORDER BY " + sortField + " " + direction;
            params = new Object[]{driveId, parentId};
        }

        return Node.<Node>find(hql, params)
                .page(page, pageSize)
                .list();
    }

    /**
     * Count children of a parent node within a drive.
     */
    public Uni<Long> countChildren(Long driveId, Long parentId) {
        if (parentId == null || parentId == 0) {
            return Node.count("drive.id = ?1 AND parentId IS NULL", driveId);
        }
        return Node.count("drive.id = ?1 AND parentId = ?2", driveId, parentId);
    }

    /**
     * Get ancestors from root to the given node (inclusive), ordered root-first.
     */
    public Uni<List<Node>> getAncestors(Node node) {
        List<Node> ancestors = new ArrayList<>();
        return walkAncestors(node, ancestors)
                .map(v -> {
                    Collections.reverse(ancestors);
                    return ancestors;
                });
    }

    private Uni<Void> walkAncestors(Node node, List<Node> ancestors) {
        ancestors.add(node);
        if (node.parentId == null) {
            return Uni.createFrom().voidItem();
        }
        return Node.<Node>find("id", node.parentId).firstResult()
                .flatMap(parent -> {
                    if (parent == null) {
                        return Uni.createFrom().voidItem();
                    }
                    return walkAncestors(parent, ancestors);
                });
    }

    /**
     * Build a tree structure starting from a list of root children, up to maxDepth.
     */
    public Uni<List<TreeEntry>> buildChildren(Long driveId, Long parentId, int maxDepth, int currentDepth) {
        if (currentDepth >= maxDepth) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        return getChildren(driveId, parentId, 0, 1000, "name", true)
                .flatMap(children -> {
                    if (children.isEmpty()) {
                        return Uni.createFrom().item(Collections.<TreeEntry>emptyList());
                    }

                    List<Uni<TreeEntry>> entryUnis = new ArrayList<>();
                    for (Node child : children) {
                        Uni<TreeEntry> entryUni = buildChildren(driveId, child.id, maxDepth, currentDepth + 1)
                                .map(subChildren -> new TreeEntry(child, subChildren));
                        entryUnis.add(entryUni);
                    }

                    return Uni.join().all(entryUnis).andCollectFailures();
                });
    }

    /**
     * Move a node to a new parent, optionally renaming it. Recomputes paths.
     */
    public Uni<Node> moveNode(Node node, Long newParentId, String newName) {
        if (newParentId != null && newParentId != 0) {
            node.parentId = newParentId;
        } else {
            node.parentId = null;
        }
        if (newName != null && !newName.isBlank()) {
            node.name = newName;
        }
        node.updatedAt = Instant.now();

        return computePath(node)
                .flatMap(newPath -> {
                    node.path = newPath;
                    return Panache.withTransaction(() ->
                            node.<Node>persist()
                                    .flatMap(persisted -> recomputeDescendantPaths(persisted).replaceWith(persisted))
                    );
                });
    }

    /**
     * Copy a node to a target parent. If deep and folder, recursively copies children.
     */
    public Uni<Node> copyNode(Node source, Long targetParentId, String newName, boolean deep) {
        Node copy = new Node();
        copy.nodeId = UUID.randomUUID().toString();
        copy.drive = source.drive;
        copy.name = (newName != null && !newName.isBlank()) ? newName : source.name;
        copy.contentType = source.contentType;
        copy.sizeBytes = source.sizeBytes;
        copy.s3Key = source.s3Key;
        copy.s3Etag = source.s3Etag;
        copy.sha256Hash = source.sha256Hash;
        copy.nodeTypeId = source.nodeTypeId;
        copy.metadata = source.metadata;
        copy.status = "ACTIVE";
        copy.createdAt = Instant.now();
        copy.updatedAt = Instant.now();

        if (targetParentId != null && targetParentId != 0) {
            copy.parentId = targetParentId;
        } else {
            copy.parentId = null;
        }

        return computePath(copy)
                .flatMap(path -> {
                    copy.path = path;
                    return Panache.withTransaction(() -> copy.<Node>persist());
                })
                .flatMap(persisted -> {
                    if (deep && source.nodeTypeId != null && source.nodeTypeId == 1) {
                        // Folder: recursively copy children
                        return copyChildren(source.drive.id, source.id, persisted.id, persisted.drive)
                                .replaceWith(persisted);
                    }
                    return Uni.createFrom().item(persisted);
                });
    }

    private Uni<Void> copyChildren(Long driveId, Long sourceParentId, Long targetParentId,
                                    ai.pipestream.repository.entity.Drive drive) {
        return getChildren(driveId, sourceParentId, 0, 10000, "name", true)
                .flatMap(children -> {
                    if (children.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    }
                    List<Uni<Void>> copyUnis = new ArrayList<>();
                    for (Node child : children) {
                        Uni<Void> copyUni = copyNode(child, targetParentId, null, true).replaceWithVoid();
                        copyUnis.add(copyUni);
                    }
                    return Uni.join().all(copyUnis).andCollectFailures().replaceWithVoid();
                });
    }

    /**
     * Delete a node. If recursive, deletes all descendants first.
     */
    public Uni<Integer> deleteNode(Node node, boolean recursive) {
        if (recursive) {
            return Panache.withTransaction(() ->
                    deleteDescendants(node.drive.id, node.id)
                            .flatMap(descendantCount ->
                                    node.delete().replaceWith(descendantCount + 1)
                            )
            );
        }
        return Panache.withTransaction(() ->
                node.delete().replaceWith(1)
        );
    }

    private Uni<Integer> deleteDescendants(Long driveId, Long parentId) {
        return Node.<Node>find("drive.id = ?1 AND parentId = ?2", driveId, parentId).list()
                .flatMap(children -> {
                    if (children.isEmpty()) {
                        return Uni.createFrom().item(0);
                    }
                    List<Uni<Integer>> deleteUnis = new ArrayList<>();
                    for (Node child : children) {
                        deleteUnis.add(
                                deleteDescendants(driveId, child.id)
                                        .flatMap(count -> child.delete().replaceWith(count + 1))
                        );
                    }
                    return Uni.join().all(deleteUnis).andCollectFailures()
                            .map(counts -> counts.stream().mapToInt(Integer::intValue).sum());
                });
    }

    /**
     * Recompute paths for all descendants of a node after a move/rename.
     */
    public Uni<Void> recomputeDescendantPaths(Node node) {
        return Node.<Node>find("drive.id = ?1 AND parentId = ?2", node.drive.id, node.id).list()
                .flatMap(children -> {
                    if (children.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    }
                    List<Uni<Void>> updates = new ArrayList<>();
                    for (Node child : children) {
                        String childPath = node.path + "/" + child.name;
                        child.path = childPath;
                        child.updatedAt = Instant.now();
                        updates.add(
                                child.<Node>persist()
                                        .flatMap(persisted -> recomputeDescendantPaths(persisted))
                        );
                    }
                    return Uni.join().all(updates).andCollectFailures().replaceWithVoid();
                });
    }

    /**
     * Compute the full path for a node by walking the parent chain.
     */
    public Uni<String> computePath(Node node) {
        if (node.parentId == null) {
            return Uni.createFrom().item("/" + node.name);
        }
        return Node.<Node>find("id", node.parentId).firstResult()
                .flatMap(parent -> {
                    if (parent == null) {
                        return Uni.createFrom().item("/" + node.name);
                    }
                    if (parent.path != null && !parent.path.isBlank()) {
                        return Uni.createFrom().item(parent.path + "/" + node.name);
                    }
                    return computePath(parent).map(parentPath -> parentPath + "/" + node.name);
                });
    }

    /**
     * Delete all nodes for a drive. Returns the count deleted.
     */
    public Uni<Long> deleteAllForDrive(Long driveId) {
        return Panache.withTransaction(() ->
                Node.delete("drive.id", driveId)
        );
    }

    /**
     * Count nodes by type for a drive.
     */
    public Uni<Long> countByDriveAndType(Long driveId, Long nodeTypeId) {
        return Node.count("drive.id = ?1 AND nodeTypeId = ?2", driveId, nodeTypeId);
    }

    /**
     * Find all nodes for a drive (for format listing).
     */
    public Uni<List<Node>> findAllByDrive(Long driveId) {
        return Node.<Node>find("drive.id", driveId).list();
    }

    /**
     * Simple tree entry for building tree responses.
     */
    public record TreeEntry(Node node, List<TreeEntry> children) {}
}
