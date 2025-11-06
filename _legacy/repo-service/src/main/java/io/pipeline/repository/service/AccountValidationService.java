package io.pipeline.repository.service;

import io.pipeline.dynamic.grpc.client.DynamicGrpcClientFactory;
import io.pipeline.repository.account.GetAccountRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Service for validating accounts via gRPC calls to account-manager.
 * <p>
 * This service provides the boundary between repo-service and account-manager,
 * ensuring proper microservice architecture without database coupling.
 * <p>
 * Uses dynamic-grpc with Stork service discovery to locate account-manager.
 */
@ApplicationScoped
public class AccountValidationService {

    private static final Logger LOG = Logger.getLogger(AccountValidationService.class);
    private static final String ACCOUNT_SERVICE_NAME = "account-manager";

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Validate that an account exists and is active.
     * <p>
     * Calls account-manager's GetAccount RPC via dynamic-grpc service discovery.
     *
     * @param accountId The account ID to validate
     * @return Uni that completes successfully if account exists and is active,
     *         fails with RuntimeException if account not found or inactive
     */
    public Uni<Void> validateAccountExistsAndActive(String accountId) {
        LOG.debugf("Validating account exists and is active: %s", accountId);

        return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .flatMap(account -> {
                if (!account.getActive()) {
                    LOG.warnf("Account %s exists but is inactive", accountId);
                    return Uni.createFrom().failure(
                        new RuntimeException("Account is inactive: " + accountId)
                    );
                }
                LOG.debugf("Account %s validated successfully", accountId);
                return Uni.createFrom().voidItem();
            })
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                // Cast to StatusRuntimeException
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                // Check if it's NOT_FOUND from account service
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return new RuntimeException("Account not found: " + accountId);
                }
                // Propagate other gRPC errors (UNAVAILABLE, etc.)
                LOG.errorf(sre, "Failed to validate account %s", accountId);
                return sre;
            });
    }

    /**
     * Validate that an account exists (active or inactive).
     * <p>
     * Used when we need to check existence but don't care about active status.
     *
     * @param accountId The account ID to validate
     * @return Uni that completes successfully if account exists,
     *         fails with RuntimeException if not found
     */
    public Uni<Void> validateAccountExists(String accountId) {
        LOG.debugf("Validating account exists: %s", accountId);

        return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .replaceWithVoid()
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return new RuntimeException("Account not found: " + accountId);
                }
                LOG.errorf(sre, "Failed to validate account %s", accountId);
                return sre;
            });
    }
}
