package io.pipeline.repository.services;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple S3 service for storing protobuf files and other assets.
 * Follows our documented design: one file per document, simple key structure.
 */
@ApplicationScoped
public class S3Service {
    
    private static final Logger LOG = Logger.getLogger(S3Service.class);
    
    // Client cache for different buckets/regions
    private final Map<String, S3Client> clientCache = new ConcurrentHashMap<>();
    
    @ConfigProperty(name = "quarkus.s3.aws.region", defaultValue = "us-east-1")
    String defaultRegion;
    
    @ConfigProperty(name = "quarkus.s3.endpoint-override")
    String endpointOverride;
    
    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.access-key-id")
    String defaultAccessKey;
    
    @ConfigProperty(name = "quarkus.s3.aws.credentials.static-provider.secret-access-key")
    String defaultSecretKey;
    
    @ConfigProperty(name = "quarkus.s3.path-style-access", defaultValue = "false")
    boolean pathStyleAccess;

    /**
     * Store a file stream in S3.
     *
     * @param bucketName S3 bucket name
     * @param s3Key The key to store the object under
     * @param inputStream The stream of data to upload
     * @param contentLength The total length of the stream
     * @param contentType Content type (e.g., "application/x-protobuf")
     * @return S3 metadata about the stored object
     */
    public S3ObjectMetadata storeStream(String bucketName, String s3Key, InputStream inputStream, long contentLength, String contentType) {
        LOG.debugf("Storing stream: bucket=%s, s3Key=%s, size=%d", bucketName, s3Key, contentLength);

        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);

        try {
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType)
                .contentLength(contentLength)
                .metadata(Map.of(
                    "s3-key", s3Key,
                    "file-type", "stream",
                    "stored-at", Instant.now().toString()
                ))
                .build();

            // Use RequestBody.fromInputStream to stream the upload
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromInputStream(inputStream, contentLength));

            LOG.debugf("Successfully stored stream: %s (ETag: %s)", s3Key, response.eTag());

            return new S3ObjectMetadata(
                s3Key,
                response.eTag(),
                contentLength,
                Instant.now(),
                contentType
            );

        } catch (Exception e) {
            LOG.errorf(e, "Failed to store stream: bucket=%s, s3Key=%s", bucketName, s3Key);
            // It's important to close the input stream on failure to prevent resource leaks
            try {
                inputStream.close();
            } catch (IOException ioException) {
                LOG.errorf(ioException, "Failed to close input stream for s3Key: %s", s3Key);
            }
            throw new RuntimeException("Failed to store stream in S3", e);
        }
    }
    
    /**
     * Store a protobuf file in S3.
     * 
     * @param bucketName S3 bucket name
     * @param documentId Document ID (used as S3 key)
     * @param protobufBytes Serialized protobuf bytes
     * @param contentType Content type (e.g., "application/x-protobuf")
     * @return S3 metadata about the stored object
     */
    public S3ObjectMetadata storeProtobuf(String bucketName, String documentId, byte[] protobufBytes, String contentType) {
        LOG.debugf("Storing protobuf: bucket=%s, documentId=%s, size=%d", bucketName, documentId, protobufBytes.length);
        
        String s3Key = documentId + ".pb";
        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);
        
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType)
                .contentLength((long) protobufBytes.length)
                .metadata(Map.of(
                    "document-id", documentId,
                    "file-type", "protobuf",
                    "stored-at", Instant.now().toString()
                ))
                .build();
            
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromBytes(protobufBytes));
            
            LOG.debugf("Successfully stored protobuf: %s (ETag: %s)", s3Key, response.eTag());
            
            return new S3ObjectMetadata(
                s3Key,
                response.eTag(),
                (long) protobufBytes.length,
                Instant.now(),
                contentType
            );
            
        } catch (Exception e) {
            LOG.errorf(e, "Failed to store protobuf: bucket=%s, documentId=%s", bucketName, documentId);
            throw new RuntimeException("Failed to store protobuf in S3", e);
        }
    }
    
    /**
     * Retrieve a protobuf file from S3.
     * 
     * @param bucketName S3 bucket name
     * @param documentId Document ID
     * @return Protobuf bytes
     */
    public byte[] retrieveProtobuf(String bucketName, String documentId) {
        LOG.debugf("Retrieving protobuf: bucket=%s, documentId=%s", bucketName, documentId);
        
        String s3Key = documentId + ".pb";
        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);
        
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
            
            ResponseBytes<GetObjectResponse> response = s3Client.getObjectAsBytes(request);
            byte[] protobufBytes = response.asByteArray();
            
            LOG.debugf("Successfully retrieved protobuf: %s (size: %d)", s3Key, protobufBytes.length);
            
            return protobufBytes;
            
        } catch (NoSuchKeyException e) {
            LOG.warnf("Protobuf not found: bucket=%s, documentId=%s", bucketName, documentId);
            throw new RuntimeException("Protobuf not found: " + documentId, e);
        } catch (Exception e) {
            LOG.errorf(e, "Failed to retrieve protobuf: bucket=%s, documentId=%s", bucketName, documentId);
            throw new RuntimeException("Failed to retrieve protobuf from S3", e);
        }
    }
    
    /**
     * Delete a protobuf file from S3.
     * 
     * @param bucketName S3 bucket name
     * @param documentId Document ID
     */
    public void deleteProtobuf(String bucketName, String documentId) {
        LOG.debugf("Deleting protobuf: bucket=%s, documentId=%s", bucketName, documentId);
        
        String s3Key = documentId + ".pb";
        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);
        
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
            
            s3Client.deleteObject(request);
            
            LOG.debugf("Successfully deleted protobuf: %s", s3Key);
            
        } catch (Exception e) {
            LOG.errorf(e, "Failed to delete protobuf: bucket=%s, documentId=%s", bucketName, documentId);
            throw new RuntimeException("Failed to delete protobuf from S3", e);
        }
    }
    
    /**
     * Check if a protobuf file exists in S3.
     * 
     * @param bucketName S3 bucket name
     * @param documentId Document ID
     * @return true if file exists
     */
    public boolean protobufExists(String bucketName, String documentId) {
        String s3Key = documentId + ".pb";
        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);
        
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
            
            s3Client.headObject(request);
            return true;
            
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            LOG.warnf("Error checking protobuf existence: bucket=%s, documentId=%s, error=%s", 
                bucketName, documentId, e.getMessage());
            return false;
        }
    }
    
    /**
     * Get S3 object metadata.
     * 
     * @param bucketName S3 bucket name
     * @param documentId Document ID
     * @return S3 metadata or null if not found
     */
    public S3ObjectMetadata getProtobufMetadata(String bucketName, String documentId) {
        String s3Key = documentId + ".pb";
        S3Client s3Client = getOrCreateClient(bucketName, null, null, null);
        
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
            
            HeadObjectResponse response = s3Client.headObject(request);
            
            return new S3ObjectMetadata(
                s3Key,
                response.eTag(),
                response.contentLength(),
                response.lastModified(),
                response.contentType()
            );
            
        } catch (NoSuchKeyException e) {
            return null;
        } catch (Exception e) {
            LOG.errorf(e, "Failed to get protobuf metadata: bucket=%s, documentId=%s", bucketName, documentId);
            throw new RuntimeException("Failed to get protobuf metadata from S3", e);
        }
    }
    
    /**
     * Create S3 bucket if it doesn't exist.
     * 
     * @param bucketName S3 bucket name
     * @param region AWS region (optional)
     */
    public void createBucketIfNotExists(String bucketName, String region) {
        S3Client s3Client = getOrCreateClient(bucketName, region, null, null);
        
        try {
            // Check if bucket exists
            HeadBucketRequest headRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
            
            s3Client.headBucket(headRequest);
            LOG.debugf("Bucket already exists: %s", bucketName);
            
        } catch (NoSuchBucketException e) {
            // Bucket doesn't exist, create it
            try {
                CreateBucketRequest createRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
                
                s3Client.createBucket(createRequest);
                LOG.infof("Created S3 bucket: %s", bucketName);
                
            } catch (Exception createException) {
                LOG.errorf(createException, "Failed to create bucket: %s", bucketName);
                throw new RuntimeException("Failed to create S3 bucket: " + bucketName, createException);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Error checking bucket existence: %s", bucketName);
            throw new RuntimeException("Error checking S3 bucket: " + bucketName, e);
        }
    }
    
    /**
     * Get or create S3 client for a bucket.
     * Uses default credentials unless custom ones are provided.
     */
    private S3Client getOrCreateClient(String bucketName, String region, String accessKey, String secretKey) {
        String cacheKey = bucketName + ":" + (region != null ? region : defaultRegion);
        
        return clientCache.computeIfAbsent(cacheKey, k -> {
            LOG.debugf("Creating S3 client: bucket=%s, region=%s", bucketName, region != null ? region : defaultRegion);
            
            var builder = S3Client.builder();
            
            // Set region
            Region awsRegion = Region.of(region != null ? region : defaultRegion);
            builder.region(awsRegion);
            
            // Set credentials
            if (accessKey != null && secretKey != null) {
                builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
                LOG.debugf("Using custom credentials for bucket: %s", bucketName);
            } else if (defaultAccessKey != null && defaultSecretKey != null) {
                builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(defaultAccessKey, defaultSecretKey)));
                LOG.debugf("Using default credentials for bucket: %s", bucketName);
            }
            
            // Set endpoint override (for MinIO)
            if (endpointOverride != null && !endpointOverride.isEmpty()) {
                builder.endpointOverride(URI.create(endpointOverride));
                LOG.debugf("Using custom endpoint: %s", endpointOverride);
            }
            
            // Set path style access (for MinIO)
            if (pathStyleAccess) {
                builder.forcePathStyle(true);
            }
            
            S3Client client = builder.build();
            LOG.debugf("Created S3 client for bucket: %s", bucketName);
            
            return client;
        });
    }
    
    /**
     * Clear client cache (useful for testing).
     */
    public void clearClientCache() {
        LOG.info("Clearing S3 client cache");
        
        clientCache.forEach((cacheKey, client) -> {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warnf("Failed to close S3 client for %s: %s", cacheKey, e.getMessage());
            }
        });
        
        clientCache.clear();
    }
    
    /**
     * S3 object metadata holder.
     */
    public static class S3ObjectMetadata {
        public final String s3Key;
        public final String eTag;
        public final Long size;
        public final Instant lastModified;
        public final String contentType;
        
        public S3ObjectMetadata(String s3Key, String eTag, Long size, Instant lastModified, String contentType) {
            this.s3Key = s3Key;
            this.eTag = eTag;
            this.size = size;
            this.lastModified = lastModified;
            this.contentType = contentType;
        }
        
        @Override
        public String toString() {
            return "S3ObjectMetadata{" +
                    "s3Key='" + s3Key + '\'' +
                    ", eTag='" + eTag + '\'' +
                    ", size=" + size +
                    ", lastModified=" + lastModified +
                    ", contentType='" + contentType + '\'' +
                    '}';
        }
    }
}