package io.pipeline.repository.exception;

/**
 * Thrown when a requested drive cannot be found.
 */
public class DriveNotFoundException extends RepoServiceException {
    
    public DriveNotFoundException(String driveName) {
        super("DRIVE_NOT_FOUND", "findDrive", "Drive not found: " + driveName);
    }
    
    public DriveNotFoundException(Long driveId) {
        super("DRIVE_NOT_FOUND", "findDrive", "Drive not found with ID: " + driveId);
    }
}
