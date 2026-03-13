package ai.pipestream.repository.account;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(RepositoryWireMockTestResource.class)
class AccountCacheServiceTest {

    @Inject
    AccountCacheService accountCacheService;

    @BeforeEach
    void setUp() {
        accountCacheService.resetCache();
    }

    @Test
    @RunOnVertxContext
    void cacheMissIsUpdatedByAccountEvent(UniAsserter asserter) {
        String accountId = "cache-miss-" + System.currentTimeMillis();

        asserter.assertThat(
            () -> accountCacheService.isValidAccount(accountId),
            initiallyValid -> {
                assertFalse(initiallyValid, "Cache miss should return false before account event");
            }
        );

        asserter.assertThat(
            () -> {
                AccountEvent event = AccountEvent.newBuilder()
                    .setAccountId(accountId)
                    .setCreated(AccountEvent.Created.newBuilder()
                        .setName("Cache Miss Account")
                        .build())
                    .build();
                return accountCacheService.handleAccountEvent(Message.of(event));
            },
            result -> { /* ack completed */ }
        );

        asserter.assertThat(
            () -> accountCacheService.isValidAccount(accountId),
            afterEventValid -> {
                assertTrue(afterEventValid, "Cache should be updated by account event after miss");
            }
        );
    }
}
