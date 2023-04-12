package liquibase.ext.neo4j.database;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static liquibase.ext.neo4j.database.jdbc.SupportedJdbcUrl.IS_SUPPORTED_JDBC_URL;
import static liquibase.ext.neo4j.lockservice.Exceptions.convertToRuntimeException;
import static liquibase.ext.neo4j.lockservice.Exceptions.messageContaining;
import static liquibase.ext.neo4j.lockservice.Exceptions.onThrow;

public class Neo4jDatabase extends AbstractJdbcDatabase {

    // field name are set to upper case, just to align on the behavior of liquibase.executor.Executor.queryForList(liquibase.statement.SqlStatement)
    // which populates the Map for each row with upper case Strings anyway
    private static final String SERVER_VERSION_QUERY =
            "CALL dbms.components() YIELD name, edition, versions WHERE name = \"Neo4j Kernel\" RETURN edition, versions[0] AS version LIMIT 1";

    private static final Predicate<Exception> CONSTRAINT_ALREADY_EXISTS_ERROR = messageContaining("constraint already exists");

    private static final Predicate<Exception> NO_SUCH_CONSTRAINT_ERROR = messageContaining("no such constraint");

    private String neo4jVersion;

    private String neo4jEdition;

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);
        initializeServerAttributes();
    }

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
        if (IS_SUPPORTED_JDBC_URL.test(url)) {
            return "liquibase.ext.neo4j.database.jdbc.Neo4jDriver";
        }
        return null;
    }

    @Override
    public String getShortName() {
        return "neo4j";
    }

    @Override
    public Integer getDefaultPort() {
        return 7687;
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
    public boolean supportsCatalogs() {
        return neo4jVersion.startsWith("4") || neo4jVersion.startsWith("5");
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    public void createUniqueConstraint(String name, String label, String property) throws DatabaseException {
        String neo4jVersion = this.getNeo4jVersion();
        if (neo4jVersion.startsWith("3.5")) {
            createUniqueConstraintForNeo4j3(label, property);
        } else if (neo4jVersion.startsWith("4")) {
            createUniqueConstraintForNeo4j4(name, label, property);
        } else if (neo4jVersion.startsWith("5")) {
            createUniqueConstraintForNeo4j5(name, label, property);
        } else {
            throw new DatabaseException(String.format(
                    "Unique constraint creation for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    neo4jVersion
            ));
        }
    }

    public void createNodeKeyConstraint(String name, String label, String firstProperty, String... rest) throws DatabaseException {
        if (!this.isEnterprise()) {
            return;
        }
        String neo4jVersion = this.getNeo4jVersion();
        String[] properties = Arrays.prepend(firstProperty, rest, String.class);
        if (neo4jVersion.startsWith("3.5")) {
            createNodeKeyConstraintForNeo4j3(label, properties);
        } else if (neo4jVersion.startsWith("4")) {
            createNodeKeyConstraintForNeo4j4(name, label, properties);
        } else if (neo4jVersion.startsWith("5")) {
            createNodeKeyConstraintForNeo4j5(name, label, properties);
        } else {
            throw new DatabaseException(String.format(
                    "Node key constraint creation for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    String.join(", ", properties),
                    neo4jVersion
            ));
        }
    }

    public void dropUniqueConstraint(String name, String label, String property) throws DatabaseException {
        String neo4jVersion = this.getNeo4jVersion();
        if (neo4jVersion.startsWith("3.5")) {
            dropUniqueConstraintForNeo4j3(label, property);
        } else if (neo4jVersion.startsWith("4")) {
            dropConstraintForNeo4j4(name);
        } else if (neo4jVersion.startsWith("5")) {
            dropConstraintForNeo4j5(name);
        } else {
            throw new DatabaseException(String.format(
                    "Unique constraint removal for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    neo4jVersion
            ));
        }
    }

    public void dropNodeKeyConstraint(String name, String label, String firstProperty, String... rest) throws DatabaseException {
        if (!this.isEnterprise()) {
            return;
        }
        String neo4jVersion = this.getNeo4jVersion();
        String[] properties = Arrays.prepend(firstProperty, rest, String.class);
        if (neo4jVersion.startsWith("3.5")) {
            dropNodeKeyConstraintForNeo4j3(label, properties);
        } else if (neo4jVersion.startsWith("4")) {
            dropConstraintForNeo4j4(name);
        } else if (neo4jVersion.startsWith("5")) {
            dropConstraintForNeo4j5(name);
        } else {
            throw new DatabaseException(String.format(
                    "Node key constraint removal for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    String.join(", ", properties),
                    neo4jVersion
            ));
        }
    }

    public void execute(SqlStatement statement) throws LiquibaseException {
        jdbcExecutor().execute(statement);
    }

    public List<Map<String, ?>> run(SqlStatement statement) throws LiquibaseException {
        return jdbcExecutor().queryForList(statement);
    }

    public String getNeo4jVersion() {
        return neo4jVersion;
    }

    public boolean isEnterprise() {
        return neo4jEdition.equals("enterprise");
    }

    private void initializeServerAttributes() {
        Map<String, ?> components = readComponents();
        this.neo4jVersion = (String) components.get("version");
        this.neo4jEdition = ((String) components.get("edition")).toLowerCase(Locale.ENGLISH);
    }

    private Map<String, ?> readComponents() {
        try {
            List<Map<String, ?>> result = jdbcExecutor().queryForList(new RawSqlStatement(SERVER_VERSION_QUERY));
            this.rollback();
            int size = result.size();
            if (size != 1) {
                throw new RuntimeException(String.format("Server components should return a single row, got %d, aborting now.", size));
            }
            return result.iterator().next();
        } catch (DatabaseException e) {
            throw new RuntimeException("Cannot read server components, aborting now.", e);
        }
    }

    // before 4.x, constraints cannot be given names
    private void createUniqueConstraintForNeo4j3(String label, String property) {
        onThrow(this::rollbackIfConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void createUniqueConstraintForNeo4j4(String name, String label, String property) {
        // `CREATE CONSTRAINT IF NOT EXISTS` is only available with Neo4j 4.2+
        onThrow(this::rollbackIfConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT `%s` ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", name, label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void createUniqueConstraintForNeo4j5(String name, String label, String property) {
        try {
            this.execute(new RawSqlStatement(String.format("CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE n.`%s` IS UNIQUE", name, label, property)));
            this.commit();
        } catch (LiquibaseException e) {
            rollbackOnError(e);
        }
    }

    // before 4.x, constraints cannot be given names

    private void createNodeKeyConstraintForNeo4j3(String label, String[] properties) {
        String list = stream(properties).map(p -> String.format("n.`%s`", p)).collect(Collectors.joining(", ", "(", ")"));
        onThrow(this::rollbackIfConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT ON (n:`%s`) ASSERT %s IS NODE KEY", label, list));
                    this.execute(sql);
                    this.commit();
                }
        );
    }
    private void createNodeKeyConstraintForNeo4j4(String name, String label, String[] properties) {
        // `CREATE CONSTRAINT IF NOT EXISTS` is only available with Neo4j 4.2+
        String list = stream(properties).map(p -> String.format("n.`%s`", p)).collect(Collectors.joining(", ", "(", ")"));
        onThrow(this::rollbackIfConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT `%s` ON (n:`%s`) ASSERT %s IS NODE KEY", name, label, list));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void createNodeKeyConstraintForNeo4j5(String name, String label, String[] properties) {
        String list = stream(properties).map(p -> String.format("n.`%s`", p)).collect(Collectors.joining(", ", "(", ")"));
        try {
            RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE %s IS NODE KEY", name, label, list));
            this.execute(sql);
            this.commit();
        } catch (LiquibaseException e) {
            rollbackOnError(e);
        }
    }


    // before 4.x, constraints cannot be given names
    private void dropUniqueConstraintForNeo4j3(String label, String property) {
        onThrow(this::rollbackIfNoSuchConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("DROP CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    // before 4.x, constraints cannot be given names
    private void dropNodeKeyConstraintForNeo4j3(String label, String[] properties) {
        String list = stream(properties).map(p -> String.format("n.`%s`", p)).collect(Collectors.joining(", ", "(", ")"));
        onThrow(this::rollbackIfNoSuchConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("DROP CONSTRAINT ON (n:`%s`) ASSERT %s IS NODE KEY", label, list));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void dropConstraintForNeo4j4(String name) {
        // `DROP CONSTRAINT IF EXISTS` is only available with Neo4j 4.2+
        onThrow(this::rollbackIfNoSuchConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("DROP CONSTRAINT `%s`", name));
                    this.execute(sql);
                    this.commit();
                }
        );
    }
    private void dropConstraintForNeo4j5(String name) {
        try {
            RawSqlStatement sql = new RawSqlStatement(String.format("DROP CONSTRAINT `%s` IF EXISTS", name));
            this.execute(sql);
            this.commit();
        } catch (LiquibaseException e) {
            rollbackOnError(e);
        }
    }

    private Executor jdbcExecutor() {
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this);
    }

    private void rollbackIfConstraintExists(Exception ex) throws DatabaseException {
        if (!CONSTRAINT_ALREADY_EXISTS_ERROR.test(ex)) {
            return;
        }
        this.rollback();
    }

    private void rollbackIfNoSuchConstraintExists(Exception ex) throws DatabaseException {
        if (!NO_SUCH_CONSTRAINT_ERROR.test(ex)) {
            return;
        }
        this.rollback();
    }

    private void rollbackOnError(LiquibaseException le) {
        try {
            this.rollback();
        } catch (DatabaseException ex) {
            le.addSuppressed(ex);
        }
        throw convertToRuntimeException(le);
    }
}
