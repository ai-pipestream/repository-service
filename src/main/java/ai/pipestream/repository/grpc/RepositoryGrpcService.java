package ai.pipestream.repository.grpc;

import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.service.VersionControlService;
import ai.pipestream.repository.service.MetadataService;

/**
 * gRPC service implementation for Repository Service.
 * Provides remote access to repository operations.
 */
@GrpcService
public class RepositoryGrpcService {

    private static final Logger LOG = Logger.getLogger(RepositoryGrpcService.class);

    @Inject
    DocumentStorageService storageService;

    @Inject
    VersionControlService versionService;

    @Inject
    MetadataService metadataService;

    public RepositoryGrpcService() {
        LOG.info("RepositoryGrpcService initialized");
    }

    // TODO: Implement gRPC service methods
}
