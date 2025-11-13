package ai.pipestream.repository.grpc;

import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import ai.pipestream.repository.service.DocumentStorageService;
import ai.pipestream.repository.service.VersionControlService;
import ai.pipestream.repository.service.MetadataService;

/**
 * Main gRPC service implementation for Repository Service.
 *
 * This service acts as a facade that delegates to specialized sub-services:
 * - AccountGrpcService: Account management operations (may use account-service client)
 * - FilesystemGrpcService: Drive and Node operations
 * - PipeDocGrpcService: PipeDoc persistence and retrieval
 *
 * For operations that interact with external services:
 * - account-service: Use @GrpcClient for account operations
 * - connector-admin: Use @GrpcClient for connector management
 * - connector-intake-service: Use @GrpcClient for document ingestion
 *
 * Proto definitions: repository/account/, repository/filesystem/, repository/pipedoc/
 */
@GrpcService
public class RepositoryGrpcService {

    private static final Logger LOG = Logger.getLogger(RepositoryGrpcService.class);

    // Internal services
    @Inject
    DocumentStorageService storageService;

    @Inject
    VersionControlService versionService;

    @Inject
    MetadataService metadataService;

    // Specialized gRPC sub-services
    @Inject
    AccountGrpcService accountService;

    @Inject
    FilesystemGrpcService filesystemService;

    @Inject
    PipeDocGrpcService pipedocService;

    // External service clients (to be added when proto stubs are generated)
    // @GrpcClient("account-service")
    // AccountServiceGrpc.AccountServiceBlockingStub accountServiceClient;
    //
    // @GrpcClient("connector-admin")
    // ConnectorAdminGrpc.ConnectorAdminBlockingStub connectorAdminClient;
    //
    // @GrpcClient("connector-intake-service")
    // IntakeServiceGrpc.IntakeServiceBlockingStub intakeServiceClient;

    public RepositoryGrpcService() {
        LOG.info("RepositoryGrpcService initialized with sub-services");
    }

    // TODO: Add facade methods that delegate to sub-services or external clients
    // Example:
    // public Uni<CreateAccountResponse> createAccount(CreateAccountRequest request) {
    //     // Option 1: Delegate to local service if we implement it here
    //     return accountService.createAccount(request);
    //
    //     // Option 2: Delegate to external account-service via client
    //     // return Uni.createFrom().item(accountServiceClient.createAccount(request));
    // }
}
