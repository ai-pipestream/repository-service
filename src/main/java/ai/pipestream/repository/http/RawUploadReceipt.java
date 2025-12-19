package ai.pipestream.repository.http;

public record RawUploadReceipt(
        String docId,
        String checksum,
        String driveName,
        String objectKey,
        String versionId,
        String etag,
        long sizeBytes,
        String contentType,
        String filename,
        String status
) {}

