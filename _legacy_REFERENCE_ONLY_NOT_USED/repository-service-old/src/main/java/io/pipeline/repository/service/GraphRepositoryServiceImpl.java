package io.pipeline.repository.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pipeline.repository.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Clean implementation of GraphRepositoryService.
 * TODO: Implement with new simple architecture.
 */
@GrpcService
public class GraphRepositoryServiceImpl implements GraphRepositoryService {

    private static final Logger LOG = Logger.getLogger(GraphRepositoryServiceImpl.class);

    @Override
    public Uni<io.pipeline.config.v1.GraphNode> createNode(CreateNodeRequest request) {
        LOG.info("createNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.GraphNode> getNode(GetNodeRequest request) {
        LOG.info("getNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.GraphNode> updateNode(UpdateNodeRequest request) {
        LOG.info("updateNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteResponse> deleteNode(DeleteNodeRequest request) {
        LOG.info("deleteNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ListNodesResponse> listNodes(ListNodesRequest request) {
        LOG.info("listNodes called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.PipelineGraph> createGraph(CreateGraphRequest request) {
        LOG.info("createGraph called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.NetworkTopology> getGraph(GetGraphRequest request) {
        LOG.info("getGraph called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.PipelineGraph> updateGraph(UpdateGraphRequest request) {
        LOG.info("updateGraph called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteResponse> deleteGraph(DeleteGraphRequest request) {
        LOG.info("deleteGraph called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ListGraphsResponse> listGraphs(ListGraphsRequest request) {
        LOG.info("listGraphs called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.NodeLookupResponse> resolveNode(ResolveNodeRequest request) {
        LOG.info("resolveNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ResolveNextNodesResponse> resolveNextNodes(ResolveNextNodesRequest request) {
        LOG.info("resolveNextNodes called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.NodeLookupResponse> resolveCrossClusterNode(io.pipeline.config.v1.CrossClusterNodeLookup request) {
        LOG.info("resolveCrossClusterNode called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<FindNodesByTopicResponse> findNodesByInputTopic(FindNodesByTopicRequest request) {
        LOG.info("findNodesByInputTopic called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<FindNodesByTopicResponse> findNodesByOutputTopic(FindNodesByTopicRequest request) {
        LOG.info("findNodesByOutputTopic called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DetectLoopsResponse> detectLoops(DetectLoopsRequest request) {
        LOG.info("detectLoops called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.config.v1.NetworkTopology> getNetworkTopology(GetNetworkTopologyRequest request) {
        LOG.info("getNetworkTopology called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Multi<io.pipeline.config.v1.GraphUpdateNotification> subscribeToUpdates(SubscribeToUpdatesRequest request) {
        LOG.info("subscribeToUpdates called - TODO: implement");
        return Multi.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }
}