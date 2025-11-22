package io.pipeline.repository.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pipeline.repository.filesystem.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Clean implementation of FilesystemService.
 * TODO: Implement with new simple architecture.
 */
@GrpcService
public class FilesystemServiceImpl implements FilesystemService {

    private static final Logger LOG = Logger.getLogger(FilesystemServiceImpl.class);

    @Override
    public Uni<Drive> createDrive(CreateDriveRequest request) {
        LOG.info("createDrive called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<Drive> getDrive(GetDriveRequest request) {
        LOG.info("getDrive called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ListDrivesResponse> listDrives(ListDrivesRequest request) {
        LOG.info("listDrives called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteDriveResponse> deleteDrive(DeleteDriveRequest request) {
        LOG.info("deleteDrive called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<Node> createNode(CreateNodeRequest request) {
        LOG.info("createNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<Node> getNode(GetNodeRequest request) {
        LOG.info("getNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<Node> updateNode(UpdateNodeRequest request) {
        LOG.info("updateNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteNodeResponse> deleteNode(DeleteNodeRequest request) {
        LOG.info("deleteNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<GetChildrenResponse> getChildren(GetChildrenRequest request) {
        LOG.info("getChildren called - DEPRECATED: Use SearchService for tree operations");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Tree operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<GetPathResponse> getPath(GetPathRequest request) {
        LOG.info("getPath called - DEPRECATED: Use SearchService for tree operations");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Tree operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<GetTreeResponse> getTree(GetTreeRequest request) {
        LOG.info("getTree called - DEPRECATED: Use SearchService for tree operations");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Tree operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<Node> moveNode(MoveNodeRequest request) {
        LOG.info("moveNode called - DEPRECATED: Use SearchService for tree operations");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Tree operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<Node> copyNode(CopyNodeRequest request) {
        LOG.info("copyNode called - DEPRECATED: Use SearchService for tree operations");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Tree operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<SearchNodesResponse> searchNodes(SearchNodesRequest request) {
        LOG.info("searchNodes called - DEPRECATED: Use SearchService instead");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Search operations moved to SearchService")));
    }

    @Override
    @Deprecated(since = "2.0", forRemoval = true)
    public Uni<SearchDrivesResponse> searchDrives(SearchDrivesRequest request) {
        LOG.info("searchDrives called - DEPRECATED: Use SearchService instead");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Search operations moved to SearchService")));
    }

    @Override
    public Multi<MetadataExport> streamAllMetadata(StreamMetadataRequest request) {
        LOG.info("streamAllMetadata called - TODO: implement");
        return Multi.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<FormatFilesystemResponse> formatFilesystem(FormatFilesystemRequest request) {
        LOG.info("formatFilesystem called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }
}