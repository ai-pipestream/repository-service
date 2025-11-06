package io.pipeline.repository.config;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MutinyDebug {
    private static final Logger LOG = Logger.getLogger(MutinyDebug.class);

    @PostConstruct
    void init() {
        try {
            Infrastructure.setDroppedExceptionHandler(throwable ->
                LOG.error("[Mutiny] Dropped exception captured", throwable)
            );
            LOG.info("[Mutiny] Dropped-exception handler registered");
        } catch (Throwable t) {
            LOG.warn("[Mutiny] Failed to register dropped-exception handler", t);
        }
    }
}
