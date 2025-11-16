package ai.pipestream.repository.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for Phase 1 intake service (in-memory fast ACK pattern).
 * All keys are namespaced under {@code repo.intake.*}.
 */
@ConfigMapping(prefix = "repo.intake")
public interface IntakeConfiguration {

    /**
     * In-memory state configuration.
     */
    Memory memory();

    /**
     * Progress streaming configuration.
     */
    Progress progress();

    /**
     * Checksum validation configuration.
     */
    Checksum checksum();

    interface Memory {
        /**
         * Maximum number of concurrent uploads to track in memory.
         * Default: 50000.
         */
        @WithDefault("50000")
        int maxUploads();

        /**
         * Time after which an idle upload state is evicted from memory.
         * Default: 30 minutes.
         */
        @WithDefault("PT30M")
        Duration idleTtl();
    }

    interface Progress {
        /**
         * Interval for streaming progress updates.
         * Default: 500ms.
         */
        @WithDefault("PT0.5S")
        Duration interval();
    }

    interface Checksum {
        /**
         * Whether to validate checksums if provided.
         * Default: false for Phase 1.
         */
        @WithDefault("false")
        boolean validate();
    }
}
