package ai.pipestream.repository.http;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.Map;

public class MinioTestResource implements QuarkusTestResourceLifecycleManager {

    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "repo-test";

    private GenericContainer<?> minio;

    @Override
    public Map<String, String> start() {
        minio = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-01-20T14-49-07Z"))
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withCommand("server", "/data", "--console-address", ":9001")
                .withExposedPorts(9000, 9001);
        minio.start();

        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);

        createBucket(endpoint);

        return Map.of(
                "repo.s3.endpoint", endpoint,
                "repo.s3.region", "us-east-1",
                "repo.s3.access-key", ACCESS_KEY,
                "repo.s3.secret-key", SECRET_KEY,
                "repo.s3.bucket", BUCKET,
                "repo.s3.path-style-access", "true",
                "repo.s3.key-prefix", "uploads"
        );
    }

    private static void createBucket(String endpoint) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);

        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of("us-east-1"))
                .endpointOverride(URI.create(endpoint))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    @Override
    public void stop() {
        if (minio != null) {
            minio.stop();
        }
    }
}

