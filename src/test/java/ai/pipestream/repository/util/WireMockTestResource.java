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
        wireMockContainer = new GenericContainer<>(DockerImageName.parse("pipestreamai/pipestream-wiremock-server:0.1.24"))
                .withExposedPorts(50052)
                .waitingFor(Wait.forLogMessage(".*Direct Streaming gRPC Server started.*", 1));
        
        wireMockContainer.start();

        String host = wireMockContainer.getHost();
        String port = wireMockContainer.getMappedPort(50052).toString();
        String address = host + ":" + port;

        return Map.of(
            // Legacy/Direct client config
            "quarkus.grpc.clients.account-service.host", host,
            "quarkus.grpc.clients.account-service.port", port,
            
            // Stork static discovery for dynamic-grpc
            "stork.account-service.service-discovery.type", "static",
            "stork.account-service.service-discovery.address-list", address,
            
            // Registration service config
            "pipestream.registration.registration-service.host", host,
            "pipestream.registration.registration-service.port", port
        );
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}