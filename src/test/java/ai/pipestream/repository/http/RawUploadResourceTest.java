package ai.pipestream.repository.http;

import ai.pipestream.repository.entity.PipeDocRecord;
import ai.pipestream.repository.s3.S3Config;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.transaction.Transactional;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
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
class RawUploadResourceTest {

    @Inject
    S3Config s3Config;

    @Test
    void rawUploadStoresToS3AndPersistsPipeDocRecord() {
        byte[] payload = ("hello-world-".repeat(1000)).getBytes(StandardCharsets.UTF_8);

        RawUploadReceipt receipt = RestAssured.given()
                .header("x-account-id", "acct-1")
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
                .as(RawUploadReceipt.class);

        assertNotNull(receipt);
        assertNotNull(receipt.docId());
        assertEquals("default", receipt.driveName());
        assertFalse(receipt.objectKey().isBlank());
        assertEquals(payload.length, receipt.sizeBytes());
        assertEquals("STORED_PIPEDOC", receipt.status());

        // Verify S3 object exists
        headObject(receipt.objectKey(), s3Config);

        // Verify DB record exists
        PipeDocRecord record = findByDocId(receipt.docId());
        assertNotNull(record);
        assertEquals(receipt.objectKey(), record.objectKey);
        assertNotNull(record.pipedocBytes);
        assertTrue(record.pipedocBytes.length > 0);
    }

    private static void headObject(String objectKey, S3Config config) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(config.accessKey(), config.secretKey());
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

    @Transactional
    PipeDocRecord findByDocId(String docId) {
        return PipeDocRecord.find("docId", docId).firstResult();
    }
}

