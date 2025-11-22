package io.pipeline.repository.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pipeline.repository.v1.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Clean implementation of PipeDocRepositoryService.
 * TODO: Implement with new simple architecture.
 */
@GrpcService
public class PipeDocRepositoryServiceImpl implements PipeDocRepositoryService {

    private static final Logger LOG = Logger.getLogger(PipeDocRepositoryServiceImpl.class);

    @Override
    public Uni<CreatePipeDocResponse> createPipeDoc(CreatePipeDocRequest request) {
        LOG.info("createPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.data.v1.PipeDoc> getPipeDoc(GetPipeDocRequest request) {
        LOG.info("getPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<io.pipeline.data.v1.PipeDoc> updatePipeDoc(UpdatePipeDocRequest request) {
        LOG.info("updatePipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteResponse> deletePipeDoc(DeletePipeDocRequest request) {
        LOG.info("deletePipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ListPipeDocsResponse> listPipeDocs(ListPipeDocsRequest request) {
        LOG.info("listPipeDocs called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<CreateStoredPipeDocResponse> createStoredPipeDoc(CreatePipeDocRequest request) {
        LOG.info("createStoredPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<StoredPipeDoc> getStoredPipeDoc(GetPipeDocRequest request) {
        LOG.info("getStoredPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<StoredPipeDoc> updateStoredPipeDoc(UpdatePipeDocRequest request) {
        LOG.info("updateStoredPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<DeleteResponse> deleteStoredPipeDoc(DeletePipeDocRequest request) {
        LOG.info("deleteStoredPipeDoc called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ListStoredPipeDocsResponse> listStoredPipeDocs(ListPipeDocsRequest request) {
        LOG.info("listStoredPipeDocs called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<SearchPipeDocsResponse> searchPipeDocs(SearchPipeDocsRequest request) {
        LOG.info("searchPipeDocs called - DEPRECATED: Use SearchService instead");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Search operations moved to SearchService")));
    }

    @Override
    public Multi<BatchOperationResult> batchCreatePipeDocs(Multi<CreatePipeDocRequest> request) {
        LOG.info("batchCreatePipeDocs called - TODO: implement");
        return Multi.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Multi<ExportChunk> exportPipeDocs(ExportPipeDocsRequest request) {
        LOG.info("exportPipeDocs called - TODO: implement");
        return Multi.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<ImportResponse> importPipeDocs(Multi<ImportChunk> request) {
        LOG.info("importPipeDocs called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }

    @Override
    public Uni<FormatRepositoryResponse> formatPipeDocRepository(FormatRepositoryRequest request) {
        LOG.info("formatPipeDocRepository called - TODO: implement");
        return Uni.createFrom().failure(new StatusRuntimeException(Status.UNIMPLEMENTED.withDescription("Not yet implemented")));
    }
}