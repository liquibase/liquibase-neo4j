package liquibase.ext.neo4j.database;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.Map;

public class Neo4jDatabase extends AbstractJdbcDatabase {

    private static final String SERVER_VERSION_QUERY =
            "CALL dbms.components() YIELD name, versions WHERE name = \"Neo4j Kernel\" RETURN versions[0] AS version LIMIT 1";

    private static final String NEO4J_VERSION_ATTRIBUTE = "neo4j.version";

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "Neo4j";
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getDatabaseProductName().equals("Neo4j");
    }

    @Override
    public String getDefaultDriver(String url) {
        String connectionUrl = StringUtil.trimToEmpty(url);
        if (connectionUrl.startsWith("jdbc:neo4j:bolt")
                || connectionUrl.startsWith("jdbc:neo4j:neo4j")) {
            return "org.neo4j.jdbc.bolt.BoltDriver";
        }
        return null;
    }

    @Override
    public String getShortName() {
        return "neo4j";
    }

    @Override
    public Integer getDefaultPort() {
        return 7867;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);
        this.setNeo4jVersion();
    }

    public String getNeo4jVersion() {
        return String.valueOf(get(NEO4J_VERSION_ATTRIBUTE));
    }

    private void setNeo4jVersion() {
        set(NEO4J_VERSION_ATTRIBUTE, readServerVersion());
    }

    private String readServerVersion() {
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this);
        try {
            String result = executor.queryForObject(new RawSqlStatement(SERVER_VERSION_QUERY), String.class);
            this.commit();
            return result;
        } catch (DatabaseException e) {
            throw new RuntimeException("Cannot determine server version, aborting now.", e);
        }
    }
}
