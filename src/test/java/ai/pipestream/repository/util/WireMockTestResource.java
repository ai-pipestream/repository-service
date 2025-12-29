package ai.pipestream.repository.util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> wireMockContainer;

    @SuppressWarnings("resource")
    @Override
    public Map<String, String> start() {
        // NOTE: pipestream-wiremock-server exposes multiple endpoints:
        // - Port 8080: gRPC server exposing many services incl. AccountService (reflection-enabled)
        // - Port 50052: "Direct" streaming gRPC server (used for large streaming, and registration in some tests)
        wireMockContainer = new GenericContainer<>(DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.27"))
                .withExposedPorts(8080, 50052)
                .waitingFor(Wait.forLogMessage(".*WireMock Server started.*", 1))
                // Configure accounts used by repository-service tests
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ID", "valid-account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_NAME", "Valid Account")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_DESCRIPTION", "Valid account for testing")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_ACTIVE", "true")
                .withEnv("WIREMOCK_ACCOUNT_GETACCOUNT_NOTFOUND_ID", "nonexistent");
        
        wireMockContainer.start();

        String host = wireMockContainer.getHost();
        String standardPort = wireMockContainer.getMappedPort(8080).toString();
        String directPort = wireMockContainer.getMappedPort(50052).toString();
        String accountAddress = host + ":" + standardPort;

        return Map.of(
            // Legacy/Direct client config (tests may use a direct Quarkus gRPC client)
            "quarkus.grpc.clients.account-service.host", host,
            "quarkus.grpc.clients.account-service.port", standardPort,
            
            // Stork static discovery for dynamic-grpc
            "stork.account-service.service-discovery.type", "static",
            "stork.account-service.service-discovery.address-list", accountAddress,

            // Repo-service uses dynamic-grpc with the service name "account-manager"
            "stork.account-manager.service-discovery.type", "static",
            "stork.account-manager.service-discovery.address-list", accountAddress,
            
            // Registration service config - use the direct server port
            "pipestream.registration.registration-service.host", host,
            "pipestream.registration.registration-service.port", directPort
        );
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}