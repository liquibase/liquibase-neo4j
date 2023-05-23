package liquibase.ext.neo4j.database.jdbc;

public interface Neo4jTransactionState {

    boolean hasActiveTransaction();
}
