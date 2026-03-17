package ai.pipestream.repository.account;

import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.v1.Account;
import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.MutinyAccountServiceGrpc;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheResult;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import ai.pipestream.repository.service.DriveService;

/**
 * Validates account existence and active status with Caffeine TTL caching.
 * <p>
 * Uses {@code @CacheResult} for simple TTL-based caching of account lookups.
 * Kafka account events invalidate the cache entry so the next lookup gets fresh data.
 * The CREATED event also triggers auto-creation of the default drive.
 */
@ApplicationScoped
public class AccountCacheService {

    private static final Logger LOG = Logger.getLogger(AccountCacheService.class);

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @Inject
    DriveService driveService;

    @ConfigProperty(name = "repo.account.validation.enabled", defaultValue = "true")
    boolean validationEnabled;

    /**
     * Check if an account exists and is active.
     *
     * @param accountId The account ID to validate
     * @return Uni<Boolean> true if valid and active, false otherwise
     */
    public Uni<Boolean> isValidAccount(String accountId) {
        if (!validationEnabled) {
            LOG.warnf("Account validation disabled. Allowing account: %s", accountId);
            return Uni.createFrom().item(true);
        }

        if (accountId == null || accountId.isBlank()) {
            return Uni.createFrom().item(false);
        }

        return lookupAccountActive(accountId);
    }

    /**
     * Cached account active status lookup. Calls account-service via gRPC and
     * caches the result. TTL configured via quarkus.cache.caffeine."account-active".
     */
    @CacheResult(cacheName = "account-active")
    Uni<Boolean> lookupAccountActive(String accountId) {
        LOG.debugf("Account cache miss, looking up: %s", accountId);

        return grpcClientFactory.getClient("account-manager", MutinyAccountServiceGrpc::newMutinyStub)
                .flatMap(client -> client.getAccount(
                        GetAccountRequest.newBuilder()
                                .setAccountId(accountId)
                                .build()))
                .map(response -> {
                    if (response.hasAccount()) {
                        boolean active = response.getAccount().getActive();
                        LOG.debugf("Account %s lookup result: active=%s", accountId, active);
                        return active;
                    }
                    LOG.debugf("Account not found: %s", accountId);
                    return false;
                })
                .onFailure().recoverWithItem(error -> {
                    LOG.warnf("Failed to lookup account %s: %s", accountId, error.getMessage());
                    return false;
                });
    }

    /**
     * Invalidate a cached account entry so the next lookup fetches fresh data.
     */
    @CacheInvalidate(cacheName = "account-active")
    Uni<Void> invalidateAccount(String accountId) {
        LOG.debugf("Invalidated cache for account: %s", accountId);
        return Uni.createFrom().voidItem();
    }

    /**
     * Clear all cached account entries (for testing).
     */
    @CacheInvalidateAll(cacheName = "account-active")
    public Uni<Void> resetCache() {
        LOG.debug("Account cache cleared");
        return Uni.createFrom().voidItem();
    }

    /**
     * Handle account events from Kafka.
     * Invalidates the cache entry so the next validation gets fresh data.
     * Also auto-creates default drive on account creation.
     */
    @Incoming("account-events-in")
    public Uni<Void> handleAccountEvent(Message<AccountEvent> message) {
        AccountEvent event = message.getPayload();
        String accountId = event.getAccountId();

        LOG.infof("Account event %s for %s", event.getOperationCase(), accountId);

        // Invalidate cache first, then handle event-specific logic
        return invalidateAccount(accountId)
                .chain(() -> {
                    if (event.getOperationCase() == AccountEvent.OperationCase.CREATED) {
                        return driveService.getOrCreateDefaultDrive(accountId)
                                .invoke(drive -> LOG.infof("Auto-created default drive for account %s: %s", accountId, drive.driveId))
                                .onFailure().invoke(error -> LOG.warnf("Failed to auto-create default drive for account %s: %s", accountId, error.getMessage()))
                                .replaceWithVoid();
                    }
                    return Uni.createFrom().voidItem();
                })
                .onItem().transformToUni(v -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().invoke(e -> LOG.errorf(e, "Failed to process account event for %s", accountId));
    }
}
