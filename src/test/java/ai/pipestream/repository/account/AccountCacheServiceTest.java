package ai.pipestream.repository.account;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
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
    void cacheMissIsUpdatedByAccountEvent() {
        String accountId = "cache-miss-" + System.currentTimeMillis();

        boolean initiallyValid = accountCacheService.isValidAccount(accountId).await().indefinitely();
        assertFalse(initiallyValid, "Cache miss should return false before account event");

        AccountEvent event = AccountEvent.newBuilder()
            .setAccountId(accountId)
            .setCreated(AccountEvent.Created.newBuilder()
                .setName("Cache Miss Account")
                .build())
            .build();

        accountCacheService.handleAccountEvent(Message.of(event)).await().indefinitely();

        boolean afterEventValid = accountCacheService.isValidAccount(accountId).await().indefinitely();
        assertTrue(afterEventValid, "Cache should be updated by account event after miss");
    }
}
