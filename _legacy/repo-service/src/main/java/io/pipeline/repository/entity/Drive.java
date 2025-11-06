package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.persistence.LockModeType;
import java.time.OffsetDateTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

/**
 * Drive entity representing an account's storage drive.
 * Each drive maps to an S3 bucket and contains documents.
 */
@Entity
@Table(name = "drives")
public class Drive extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(unique = true, nullable = false)
    public String name;                    // Customer-facing drive name
    
    @Column(name = "bucket_name", unique = true, nullable = false)
    public String bucketName;              // Actual S3 bucket name
    
    @Column(name = "account_id", nullable = false)
    public String accountId;               // Account identifier for multi-tenancy
    
    @Column(name = "region")
    public String region;                  // S3 region (optional)
    
    @Column(name = "credentials_ref")
    public String credentialsRef;          // String reference to external secret management
    
    @Column(name = "status_id")
    public Long statusId;                  // Foreign key to drive_status lookup table
    
    @Column(name = "description")
    public String description;             // Drive description
    
    @Column(name = "created_at")
    public OffsetDateTime createdAt;
    
    @Column(name = "metadata")
    public String metadata;                 // JSON metadata for unstructured data
    
    // Constructors
    public Drive() {}
    
    public Drive(String name, String bucketName, String accountId) {
        this.name = name;
        this.bucketName = bucketName;
        this.accountId = accountId;
        this.createdAt = OffsetDateTime.now();
    }
    
    // Service class for thread-safe Drive queries
    @ApplicationScoped
    public static class DriveService {
        @Inject
        EntityManager entityManager;
        
        @Transactional
        public Drive findByName(String name) {
            try {
                return entityManager
                    .createQuery("SELECT d FROM Drive d WHERE d.name = :name", Drive.class)
                    .setParameter("name", name)
                    .getSingleResult();
            } catch (jakarta.persistence.NoResultException e) {
                return null;
            }
        }
        
        @Transactional
        public Drive findByBucketName(String bucketName) {
            try {
                return entityManager
                    .createQuery("SELECT d FROM Drive d WHERE d.bucketName = :bucketName", Drive.class)
                    .setParameter("bucketName", bucketName)
                    .getSingleResult();
            } catch (jakarta.persistence.NoResultException e) {
                return null;
            }
        }
        
        @Transactional
        public java.util.List<Drive> findByAccountId(String accountId) {
            return entityManager
                .createQuery("SELECT d FROM Drive d WHERE d.accountId = :accountId", Drive.class)
                .setParameter("accountId", accountId)
                .getResultList();
        }
    }
}
