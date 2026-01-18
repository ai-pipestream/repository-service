package ai.pipestream.repository.util;

import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import ai.pipestream.test.support.RepositoryWireMockTestResource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.eclipse.microprofile.config.ConfigProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(RepositoryWireMockTestResource.class)
public class WireMockSanityTest {

    @Test
    void canCallAccountServiceOnWireMock() {
        // RepositoryWireMockTestResource sets these properties (as Quarkus config overrides)
        String host = ConfigProvider.getConfig()
                .getOptionalValue("quarkus.grpc.clients.account-service.host", String.class)
                .orElse(null);
        Integer port = ConfigProvider.getConfig()
                .getOptionalValue("quarkus.grpc.clients.account-service.port", Integer.class)
                .orElse(null);
        
        System.out.println("Connecting to WireMock at " + host + ":" + port);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            AccountServiceGrpc.AccountServiceBlockingStub stub = AccountServiceGrpc.newBlockingStub(channel);

            GetAccountResponse response = stub.getAccount(GetAccountRequest.newBuilder()
                    .setAccountId("valid-account")
                    .build());

            System.out.println("Got account: " + response.getAccount().getName());
            
            assertEquals("valid-account", response.getAccount().getAccountId());
            assertTrue(response.getAccount().getActive());
            
        } finally {
            channel.shutdown();
        }
    }
}
