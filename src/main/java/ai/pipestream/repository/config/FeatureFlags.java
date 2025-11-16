package ai.pipestream.repository.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Feature flags for phased rollout.
 */
@ConfigMapping(prefix = "repo.features")
public interface FeatureFlags {

    /**
     * Phase 1 configuration.
     */
    Phase1 phase1();

    interface Phase1 {
        /**
         * Enable Phase 1 intake-only fast ACK path.
         * Default: false.
         */
        @WithDefault("false")
        boolean enabled();
    }
}
