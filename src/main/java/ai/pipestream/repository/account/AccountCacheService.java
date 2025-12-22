package ai.pipestream.repository.account;

import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.v1.Account;
import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.ListAccountsRequest;
import ai.pipestream.repository.account.v1.MutinyAccountServiceGrpc;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Caches account information locally for fast validation.
 *
 * Cache is populated via:
 * 1. Kafka listener for account lifecycle events
 * 2. Non-blocking startup cache warming via ListAccounts gRPC call
 * 3. On-demand gRPC lookup on cache miss
 */
@ApplicationScoped
public class AccountCacheService {

    private static final Logger LOG = Logger.getLogger(AccountCacheService.class);
    private static final int PAGE_SIZE = 100;

    private final ConcurrentHashMap<String, CachedAccount> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean warmupComplete = new AtomicBoolean(false);

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @ConfigProperty(name = "repo.account.validation.enabled", defaultValue = "true")
    boolean validationEnabled;

    /**
     * Cached account entry with active status.
     */
    public record CachedAccount(String accountId, String name, boolean active) {
        static CachedAccount from(Account account) {
            return new CachedAccount(account.getAccountId(), account.getName(), account.getActive());
        }
    }

    /**
     * Warm the cache at startup by loading all accounts.
     * Non-blocking - does not delay startup.
     */
    void onStartup(@Observes StartupEvent event) {
        LOG.info("Starting non-blocking account cache warmup...");
        warmCache("")
                .subscribe().with(
                        count -> {
                            warmupComplete.set(true);
                            LOG.infof("Account cache warmup complete. Loaded %d accounts.", count);
                        },
                        error -> {
                            warmupComplete.set(true);
                            LOG.warnf("Account cache warmup failed (will use on-demand lookup): %s", error.getMessage());
                        }
                );
    }

    /**
     * Recursively pages through all accounts to warm the cache.
     */
    private Uni<Integer> warmCache(String pageToken) {
        return grpcClientFactory.getClient("account-manager", MutinyAccountServiceGrpc::newMutinyStub)
                .flatMap(client -> {
                    ListAccountsRequest.Builder request = ListAccountsRequest.newBuilder()
                            .setPageSize(PAGE_SIZE)
                            .setIncludeInactive(false);
                    if (pageToken != null && !pageToken.isEmpty()) {
                        request.setPageToken(pageToken);
                    }
                    return client.listAccounts(request.build());
                })
                .flatMap(response -> {
                    int loaded = 0;
                    for (Account account : response.getAccountsList()) {
                        cache.put(account.getAccountId(), CachedAccount.from(account));
                        loaded++;
                    }
                    String nextToken = response.getNextPageToken();
                    if (nextToken != null && !nextToken.isEmpty()) {
                        int currentLoaded = loaded;
                        return warmCache(nextToken).map(next -> currentLoaded + next);
                    }
                    return Uni.createFrom().item(loaded);
                });
    }

    /**
     * Handle account events from Kafka.
     */
    @Incoming("account-events-in")
    public Uni<Void> handleAccountEvent(Message<AccountEvent> message) {
        AccountEvent event = message.getPayload();
        String accountId = event.getAccountId();

        switch (event.getOperationCase()) {
            case CREATED -> {
                AccountEvent.Created created = event.getCreated();
                cache.put(accountId, new CachedAccount(accountId, created.getName(), true));
                LOG.infof("Account created: %s", accountId);
            }
            case UPDATED -> {
                AccountEvent.Updated updated = event.getUpdated();
                CachedAccount existing = cache.get(accountId);
                boolean active = existing != null ? existing.active() : true;
                cache.put(accountId, new CachedAccount(accountId, updated.getName(), active));
                LOG.infof("Account updated: %s", accountId);
            }
            case INACTIVATED -> {
                CachedAccount existing = cache.get(accountId);
                if (existing != null) {
                    cache.put(accountId, new CachedAccount(accountId, existing.name(), false));
                }
                LOG.infof("Account inactivated: %s", accountId);
            }
            case REACTIVATED -> {
                CachedAccount existing = cache.get(accountId);
                if (existing != null) {
                    cache.put(accountId, new CachedAccount(accountId, existing.name(), true));
                }
                LOG.infof("Account reactivated: %s", accountId);
            }
            default -> LOG.warnf("Unknown account event operation for %s: %s", accountId, event.getOperationCase());
        }

        return Uni.createFrom().completionStage(message.ack());
    }

    /**
     * Check if an account exists and is active.
     * First checks cache, then falls back to gRPC lookup.
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

        CachedAccount cached = cache.get(accountId);
        if (cached != null) {
            return Uni.createFrom().item(cached.active());
        }

        // Cache miss - lookup via gRPC
        return lookupAccount(accountId)
                .map(account -> {
                    if (account != null) {
                        cache.put(accountId, CachedAccount.from(account));
                        return account.getActive();
                    }
                    return false;
                })
                .onFailure().recoverWithItem(error -> {
                    LOG.warnf("Failed to lookup account %s: %s", accountId, error.getMessage());
                    return false;
                });
    }

    /**
     * Lookup account via gRPC.
     */
    private Uni<Account> lookupAccount(String accountId) {
        return grpcClientFactory.getClient("account-manager", MutinyAccountServiceGrpc::newMutinyStub)
                .flatMap(client -> client.getAccount(
                        GetAccountRequest.newBuilder()
                                .setAccountId(accountId)
                                .build()))
                .map(response -> response.hasAccount() ? response.getAccount() : null)
                .onFailure().recoverWithItem((Account) null);
    }

    /**
     * Get the current cache size (for monitoring/testing).
     */
    public int cacheSize() {
        return cache.size();
    }

    /**
     * Check if warmup is complete (for monitoring/testing).
     */
    public boolean isWarmupComplete() {
        return warmupComplete.get();
    }
}
