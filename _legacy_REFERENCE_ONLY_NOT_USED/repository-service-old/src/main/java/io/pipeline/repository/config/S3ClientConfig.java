package io.pipeline.repository.config;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URI;
import java.util.Optional;

@ApplicationScoped
public class S3ClientConfig {

    @ConfigProperty(name = "quarkus.s3.endpoint-override")
    Optional<URI> endpointOverride;

    @ConfigProperty(name = "quarkus.s3.aws.region")
    String region;

    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.access-key-id")
    Optional<String> accessKeyId;

    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.secret-access-key")
    Optional<String> secretAccessKey;

    @Produces
    @ApplicationScoped
    public S3AsyncClient s3AsyncClient() {
        SdkAsyncHttpClient nettyClient = NettyNioAsyncHttpClient.builder()
                .build();

        S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .httpClient(nettyClient)
                .region(Region.US_EAST_1);

        endpointOverride.ifPresent(builder::endpointOverride);

        if (accessKeyId.isPresent() && secretAccessKey.isPresent()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId.get(), secretAccessKey.get())));
        }
        
        // This is important for MinIO
        builder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());

        return builder.build();
    }
}