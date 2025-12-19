package ai.pipestream.repository.s3;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

@ApplicationScoped
public class S3Clients {

    @Produces
    @ApplicationScoped
    public S3AsyncClient s3AsyncClient(S3Config config) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(config.accessKey(), config.secretKey());

        return S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of(config.region()))
                .endpointOverride(URI.create(config.endpoint()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(config.pathStyleAccess())
                        .build())
                .build();
    }
}

