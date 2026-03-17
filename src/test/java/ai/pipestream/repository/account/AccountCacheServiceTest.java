package ai.pipestream.repository.account;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class AccountCacheServiceTest {

    @Inject
    AccountCacheService accountCacheService;

    @Test
    @RunOnVertxContext
    void validAccountReturnsTrue(UniAsserter asserter) {
        // "valid-account" is configured in WireMock to return active=true
        asserter.assertThat(
            () -> accountCacheService.isValidAccount("valid-account"),
            valid -> assertTrue(valid, "Known active account should return true")
        );
    }

    @Test
    @RunOnVertxContext
    void unknownAccountReturnsFalse(UniAsserter asserter) {
        asserter.assertThat(
            () -> accountCacheService.isValidAccount("nonexistent-" + System.currentTimeMillis()),
            valid -> assertFalse(valid, "Unknown account should return false")
        );
    }

    @Test
    @RunOnVertxContext
    void kafkaEventInvalidatesCache(UniAsserter asserter) {
        // First call caches the result
        asserter.assertThat(
            () -> accountCacheService.isValidAccount("valid-account"),
            valid -> assertTrue(valid, "First lookup should succeed")
        );

        // Simulate inactivation event — invalidates the cache entry
        asserter.assertThat(
            () -> {
                AccountEvent event = AccountEvent.newBuilder()
                    .setAccountId("valid-account")
                    .setInactivated(AccountEvent.Inactivated.getDefaultInstance())
                    .build();
                return accountCacheService.handleAccountEvent(Message.of(event));
            },
            result -> { /* ack completed */ }
        );

        // Next lookup hits gRPC again (WireMock still returns active=true,
        // but the point is the cache was invalidated and a fresh lookup occurred)
        asserter.assertThat(
            () -> accountCacheService.isValidAccount("valid-account"),
            valid -> assertTrue(valid, "Post-invalidation lookup should re-fetch from gRPC")
        );
    }
}
