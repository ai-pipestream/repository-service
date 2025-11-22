package io.pipeline.repository.test;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Test utility for S3 bucket management.
 * This should only be used in test code, never in production.
 */
public class S3TestHelper {
    
    private final S3Client s3Client;
    
    public S3TestHelper(S3Client s3Client) {
        this.s3Client = s3Client;
    }
    
    /**
     * Create a bucket for testing purposes only.
     * In production, buckets are created by infrastructure/DevOps teams.
     */
    public void createBucketIfNotExists(String bucketName, String region) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(CreateBucketConfiguration.builder()
                            .locationConstraint(region)
                            .build())
                    .build();
            s3Client.createBucket(createBucketRequest);
        }
    }
    
    /**
     * Delete a bucket for test cleanup.
     * This will delete all objects first, then the bucket.
     */
    public void deleteBucket(String bucketName) {
        try {
            // First, delete all objects in the bucket
            deleteAllObjects(bucketName);
            
            // Then delete the bucket
            s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            // Bucket doesn't exist, that's fine
        }
    }
    
    /**
     * Delete all objects in a bucket.
     */
    private void deleteAllObjects(String bucketName) {
        try {
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucketName).build());
                
            if (!listResponse.contents().isEmpty()) {
                // Create delete request for all objects
                java.util.List<ObjectIdentifier> objectsToDelete = listResponse.contents().stream()
                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                    .collect(java.util.stream.Collectors.toList());
                    
                DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(objectsToDelete).build())
                    .build();
                    
                s3Client.deleteObjects(deleteRequest);
            }
        } catch (NoSuchBucketException e) {
            // Bucket doesn't exist, nothing to delete
        }
    }
    
    /**
     * Check if a bucket exists.
     */
    public boolean bucketExists(String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }
}
