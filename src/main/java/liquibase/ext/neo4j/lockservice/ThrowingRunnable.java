package liquibase.ext.neo4j.lockservice;

// Callable sadly does not work with void methods
@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
