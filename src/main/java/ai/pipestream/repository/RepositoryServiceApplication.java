package ai.pipestream.repository;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Main application entry point for the Repository Service.
 * Manages document storage, versioning, and metadata.
 */
@QuarkusMain
@ApplicationScoped
public class RepositoryServiceApplication implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(RepositoryServiceApplication.class);

    public static void main(String... args) {
        Quarkus.run(RepositoryServiceApplication.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        LOG.info("Repository Service started successfully");
        Quarkus.waitForExit();
        return 0;
    }
}
