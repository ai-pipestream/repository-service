package ai.pipestream.repository.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.filesystem.v1.DriveUpdateNotification;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from DriveUpdateNotification for Kafka partitioning.
 *
 * Uses the drive name to derive a deterministic UUID for partition locality.
 */
@ApplicationScoped
public class DriveUpdateKeyExtractor implements UuidKeyExtractor<DriveUpdateNotification> {

    @Override
    public UUID extractKey(DriveUpdateNotification event) {
        if (event == null || !event.hasDrive() || event.getDrive().getName().isBlank()) {
            return UUID.nameUUIDFromBytes("unknown-drive".getBytes(StandardCharsets.UTF_8));
        }
        String driveKey = event.getDrive().getAccountId() + ":" + event.getDrive().getName();
        return UUID.nameUUIDFromBytes(driveKey.getBytes(StandardCharsets.UTF_8));
    }
}
