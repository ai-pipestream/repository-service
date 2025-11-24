package ai.pipestream.repository.s3;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * CDI Producer for S3TransferManager.
 * This ensures S3TransferManager is properly configured using the existing S3AsyncClient
 * provided by Quarkus.
 */
@ApplicationScoped
public class S3TransferManagerProducer {

    /**
     * Produces an S3TransferManager instance.
     * The S3AsyncClient is injected by Quarkus, which handles its configuration.
     *
     * @param s3AsyncClient The S3AsyncClient provided by Quarkus.
     * @return A configured S3TransferManager.
     */
    @Produces
    @ApplicationScoped
    public S3TransferManager s3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }
}
