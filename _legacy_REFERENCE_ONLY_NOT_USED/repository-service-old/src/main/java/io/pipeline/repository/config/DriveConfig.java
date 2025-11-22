package io.pipeline.repository.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration for virtual drives in the filesystem.
 * Each drive is an isolated namespace, similar to Windows drive letters (C:, D:)
 * but with meaningful names instead of letters.
 * 
 * This provides complete isolation between different applications, users, or tenants.
 * No path traversal is possible between drives.
 */
@ConfigMapping(prefix = "pipeline.repository.drives")
public interface DriveConfig {
    
    /**
     * The default drive name when none is specified.
     * This is like the "C:" drive in Windows - the primary drive.
     */
    @WithDefault("main")
    String defaultDrive();
    
    /**
     * Whether to auto-create drives when they're first accessed.
     * If false, drives must be explicitly created before use.
     */
    @WithDefault("true")
    boolean autoCreate();
    
    /**
     * Drive-specific configurations.
     * Key is the drive name, value is the configuration for that drive.
     */
    Map<String, DriveSettings> drives();
    
    /**
     * Configuration for a specific drive.
     */
    interface DriveSettings {
        /**
         * Human-readable description of this drive.
         */
        Optional<String> description();
        
        /**
         * Whether this drive is read-only.
         */
        @WithDefault("false")
        boolean readOnly();
        
        /**
         * Maximum size in bytes (0 = unlimited).
         */
        @WithDefault("0")
        long maxSize();
        
        /**
         * Whether this drive is enabled.
         */
        @WithDefault("true")
        boolean enabled();
    }
}