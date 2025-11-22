package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

/**
 * Drive status lookup table for flexible status management.
 */
@Entity
@Table(name = "drive_status")
public class DriveStatus extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(unique = true, nullable = false)
    public String code;                    // Status code (ACTIVE, INACTIVE, etc.)
    
    @Column(nullable = false)
    public String description;             // Human-readable description
    
    @Column(name = "is_active")
    public Boolean isActive = true;        // Whether this status represents an active drive
    
    // Constructors
    public DriveStatus() {}
    
    public DriveStatus(String code, String description, Boolean isActive) {
        this.code = code;
        this.description = description;
        this.isActive = isActive;
    }
    
    // Helper methods
    public static DriveStatus findByCode(String code) {
        return find("code", code).firstResult();
    }
    
    public static java.util.List<DriveStatus> findActiveStatuses() {
        return find("isActive", true).list();
    }
}
