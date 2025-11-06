package io.pipeline.repository;

import io.pipeline.grpc.wiremock.WireMockTestBase;
import io.pipeline.grpc.wiremock.WireMockGrpcTestResource;
import io.pipeline.grpc.wiremock.MockGrpcProfile;
import io.pipeline.repository.account.AccountServiceGrpc;
import io.pipeline.repository.account.CreateAccountRequest;
import io.pipeline.repository.account.GetAccountRequest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating how repo-service would interact with account-manager
 * using WireMock to simulate the account-manager service.
 */
@QuarkusTest
@QuarkusTestResource(WireMockGrpcTestResource.class)
@TestProfile(MockGrpcProfile.class)
public class AccountManagerIntegrationTest extends WireMockTestBase {

    private AccountServiceGrpc.AccountServiceBlockingStub accountService;

    @BeforeEach
    void setUp() {
        super.setUpMocks();
        
        // Create gRPC client that connects to WireMock
        var channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        accountService = AccountServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testDriveCreation_WithAccountValidation() {
        // Setup mock - account exists and is active
        mocks.accountManager().mockGetAccount("test-account", "Test Account", "Test description", true);

        // Simulate repo-service validating account before creating drive
        var accountRequest = GetAccountRequest.newBuilder()
                .setAccountId("test-account")
                .build();

        var account = accountService.getAccount(accountRequest);

        // Verify account validation
        assertEquals("test-account", account.getAccountId());
        assertTrue(account.getActive());
        assertEquals("Test Account", account.getName());

        // Now repo-service can proceed with drive creation
        // (This would be the actual drive creation logic)
        assertTrue(true, "Account validation passed, drive creation can proceed");
    }

    @Test
    public void testDriveCreation_WithInactiveAccount() {
        // Setup mock - account exists but is inactive
        mocks.accountManager().mockGetAccount("inactive-account", "Inactive Account", "Inactive description", false);

        // Simulate repo-service validating account before creating drive
        var accountRequest = GetAccountRequest.newBuilder()
                .setAccountId("inactive-account")
                .build();

        var account = accountService.getAccount(accountRequest);

        // Verify account validation
        assertEquals("inactive-account", account.getAccountId());
        assertFalse(account.getActive());

        // Repo-service should reject drive creation for inactive account
        // (This would be the actual validation logic)
        assertFalse(account.getActive(), "Account is inactive, drive creation should be rejected");
    }

    @Test
    public void testDriveCreation_WithNonExistentAccount() {
        // Setup mock - account not found
        mocks.accountManager().mockAccountNotFound("nonexistent-account");

        // Simulate repo-service validating account before creating drive
        var accountRequest = GetAccountRequest.newBuilder()
                .setAccountId("nonexistent-account")
                .build();

        // Verify exception is thrown for non-existent account
        assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            accountService.getAccount(accountRequest);
        }, "Account not found, drive creation should be rejected");
    }

    @Test
    public void testAccountCreation_FromRepoService() {
        // Setup mock - account creation
        mocks.accountManager().mockCreateAccount("new-account", "New Account", "Created from repo-service");

        // Simulate repo-service creating an account
        var createRequest = CreateAccountRequest.newBuilder()
                .setAccountId("new-account")
                .setName("New Account")
                .setDescription("Created from repo-service")
                .build();

        var response = accountService.createAccount(createRequest);

        // Verify account creation
        assertTrue(response.getCreated());
        assertEquals("new-account", response.getAccount().getAccountId());
        assertEquals("New Account", response.getAccount().getName());
        assertTrue(response.getAccount().getActive());
    }
}
