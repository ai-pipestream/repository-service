package io.pipeline.repository.test;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Test component for managing S3 buckets per test.
 * Creates unique buckets based on test class and method names.
 */
@ApplicationScoped
public class TestBucketManager {
    
    private static final Logger LOG = Logger.getLogger(TestBucketManager.class);
    
    @Inject
    S3Client s3Client;
    
    private S3TestHelper s3TestHelper;
    private String currentBucket;
    
    /**
     * Initialize and create a unique bucket for the current test.
     * Call this in @BeforeEach with TestInfo parameter.
     */
    public String setupBucket(TestInfo testInfo) {
        if (s3TestHelper == null) {
            s3TestHelper = new S3TestHelper(s3Client);
        }
        
        // Create bucket name from test class and method (S3 compliant)
        String className = testInfo.getTestClass()
            .map(Class::getSimpleName)
            .map(name -> name.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase())
            .orElse("unknown");
        String methodName = testInfo.getTestMethod()
            .map(method -> method.getName().replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase())
            .orElse("unknown");
            
        // Keep it short and S3 compliant (max 63 chars)
        String timestamp = String.valueOf(System.currentTimeMillis());
        String baseName = String.format("test-%s-%s", 
            className.substring(0, Math.min(className.length(), 10)),
            methodName.substring(0, Math.min(methodName.length(), 10)));
        currentBucket = (baseName + "-" + timestamp).substring(0, Math.min(63, baseName.length() + timestamp.length() + 1));
            
        s3TestHelper.createBucketIfNotExists(currentBucket, "us-east-1");
        LOG.infof("Created test bucket: %s for %s.%s", currentBucket, className, methodName);
        
        return currentBucket;
    }
    
    /**
     * Clean up the current test bucket.
     * Call this in @AfterEach.
     */
    public void cleanupBucket() {
        if (currentBucket != null && s3TestHelper != null) {
            try {
                s3TestHelper.deleteBucket(currentBucket);
                LOG.infof("Deleted test bucket: %s", currentBucket);
            } catch (Exception e) {
                LOG.warnf("Failed to delete test bucket %s: %s", currentBucket, e.getMessage());
            } finally {
                currentBucket = null;
            }
        }
    }
    
    /**
     * Clean up a specific bucket by name (parallel-safe).
     */
    public void cleanupBucket(String bucketName) {
        if (bucketName != null && s3TestHelper != null) {
            try {
                s3TestHelper.deleteBucket(bucketName);
                LOG.infof("Deleted specific test bucket: %s", bucketName);
            } catch (Exception e) {
                LOG.warnf("Failed to delete test bucket %s: %s", bucketName, e.getMessage());
            }
        }
    }
    
    /**
     * Get the current test bucket name.
     */
    public String getCurrentBucket() {
        if (currentBucket == null) {
            throw new IllegalStateException("No bucket set up - call setupBucket() first");
        }
        return currentBucket;
    }
}
