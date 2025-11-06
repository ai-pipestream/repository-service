package io.pipeline.repository.exception;

/**
 * Thrown when S3 storage operations fail.
 */
public class S3StorageException extends RepoServiceException {
    
    public S3StorageException(String operation, String bucket, String key, Throwable cause) {
        super("S3_STORAGE_ERROR", operation, 
            String.format("S3 operation failed: bucket=%s, key=%s", bucket, key), cause);
    }
    
    public S3StorageException(String operation, String bucket, String key, String details) {
        super("S3_STORAGE_ERROR", operation, 
            String.format("S3 operation failed: bucket=%s, key=%s, details=%s", bucket, key, details));
    }
    
    public static S3StorageException uploadFailed(String bucket, String key, Throwable cause) {
        return new S3StorageException("upload", bucket, key, cause);
    }
    
    public static S3StorageException downloadFailed(String bucket, String key, Throwable cause) {
        return new S3StorageException("download", bucket, key, cause);
    }
    
    public static S3StorageException deleteFailed(String bucket, String key, Throwable cause) {
        return new S3StorageException("delete", bucket, key, cause);
    }
}
