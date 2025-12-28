package ai.pipestream.repository.http;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.s3.S3Config;
import ai.pipestream.repository.util.WireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.vertx.MutinyHelper;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(MinioTestResource.class)
@QuarkusTestResource(WireMockTestResource.class)
class RawUploadResourceTest {

    @Inject
    S3Config s3Config;

    @Inject
    Vertx vertx;

    /**
     * Uploads data; verifies S3 object and database record exist
     */
    @Test
    @RunOnVertxContext
    void rawUploadStoresToS3AndPersistsPipeDocRecord(UniAsserter asserter) {
        byte[] payload = ("hello-world-".repeat(1000)).getBytes(StandardCharsets.UTF_8);

        // Do blocking HTTP upload off the event loop
        asserter.execute(() -> Uni.createFrom().item(() -> RestAssured.given()
                        .header("x-account-id", "valid-account")
                        .header("x-connector-id", "conn-1")
                        .header("x-drive-name", "default")
                        .header("x-filename", "test.txt")
                        .header("content-type", "text/plain")
                        .body(payload)
                        .when()
                        .post("/internal/uploads/raw")
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(RawUploadReceipt.class))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                // Switch back to Vert.x event loop for subsequent reactive DB operations
                .emitOn(MutinyHelper.executor(vertx.getDelegate()))
                .invoke(receipt -> asserter.putData("receipt", receipt)));

        asserter.assertThat(() -> Uni.createFrom().item(() -> (RawUploadReceipt) asserter.getData("receipt")),
                receipt -> {
                    assertNotNull(receipt);
                    assertNotNull(receipt.docId());
                    assertEquals("default", receipt.driveName());
                    assertFalse(receipt.objectKey().isBlank());
                    assertEquals(payload.length, receipt.sizeBytes());
                    assertEquals("STORED_PIPEDOC", receipt.status());
                });

        // Verify DB record exists (reactive)
        // For initial intake, we query by docId alone (assuming initial state where graph_address_id = datasource_id)
        asserter.assertThat(() -> {
            RawUploadReceipt receipt = (RawUploadReceipt) asserter.getData("receipt");
            return Panache.withSession(() ->
                    PipeDocRecord.<PipeDocRecord>find("docId", receipt.docId()).firstResult());
        }, record -> {
            RawUploadReceipt receipt = (RawUploadReceipt) asserter.getData("receipt");
            assertNotNull(record);
            assertEquals(receipt.objectKey(), record.objectKey);
            assertEquals(receipt.driveName(), record.driveName);
            assertEquals(receipt.sizeBytes(), record.sizeBytes);
            assertEquals("valid-account", record.accountId);
            assertEquals("conn-1", record.connectorId);
            assertNotNull(record.datasourceId);
            assertFalse(record.datasourceId.isBlank());
            assertNotNull(record.pipedocObjectKey);
            assertEquals(receipt.objectKey() + ".pipedoc", record.pipedocObjectKey);

            asserter.putData("objectKey", record.objectKey);
            asserter.putData("pipedocObjectKey", record.pipedocObjectKey);
        });

        // Verify S3 objects exist (blocking) off the event loop
        asserter.execute(() -> Uni.createFrom().voidItem()
                .invoke(() -> headObject((String) asserter.getData("objectKey"), s3Config))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .emitOn(MutinyHelper.executor(vertx.getDelegate())));
        asserter.execute(() -> Uni.createFrom().voidItem()
                .invoke(() -> headObject((String) asserter.getData("pipedocObjectKey"), s3Config))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .emitOn(MutinyHelper.executor(vertx.getDelegate())));
    }

    private static void headObject(String objectKey, S3Config config) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(config.accessKey(), config.secretKey());
        // Queries S3 for object metadata using configured client
        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of(config.region()))
                .endpointOverride(URI.create(config.endpoint()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(config.pathStyleAccess()).build())
                .build()) {
            s3.headObject(HeadObjectRequest.builder()
                    .bucket(config.bucket())
                    .key(objectKey)
                    .build());
        }
    }

    // Note: DB queries in this test are performed via TransactionalUniAsserter on Vert.x event loop.
}