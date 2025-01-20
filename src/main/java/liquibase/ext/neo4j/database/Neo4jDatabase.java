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
import static liquibase.ext.neo4j.database.KernelVersion.V3_5;
import static liquibase.ext.neo4j.database.KernelVersion.V4_0;
import static liquibase.ext.neo4j.database.KernelVersion.V5_0;
import static liquibase.ext.neo4j.database.jdbc.SupportedJdbcUrl.IS_SUPPORTED_JDBC_URL;
import static liquibase.ext.neo4j.lockservice.Exceptions.convertToRuntimeException;
import static liquibase.ext.neo4j.lockservice.Exceptions.messageContaining;
import static liquibase.ext.neo4j.lockservice.Exceptions.onThrow;

public class Neo4jDatabase extends AbstractJdbcDatabase {

    private static final String SERVER_VERSION_QUERY =
            "CALL dbms.components() YIELD name, edition, versions WHERE name = \"Neo4j Kernel\" RETURN edition, versions[0] AS version LIMIT 1";

    private static final Predicate<Exception> INDEX_ALREADY_EXISTS_ERROR = messageContaining("index already exists");

    private static final Predicate<Exception> CONSTRAINT_ALREADY_EXISTS_ERROR = messageContaining("constraint already exists");

    private static final Predicate<Exception> NO_SUCH_INDEX_ERROR = messageContaining("no such index");

    private static final Predicate<Exception> NO_SUCH_CONSTRAINT_ERROR = messageContaining("no such constraint");

    private KernelVersion kernelVersion;

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
        return kernelVersion.compareTo(V4_0) >= 0 && isEnterprise();
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    public void createIndex(String name, String label, String property) throws DatabaseException {
        if (kernelVersion.compareTo(V5_0) >= 0) {
            createIndexForNeo4j5(name, label, property);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            createIndexForNeo4j4(name, label, property);
        } else  if (kernelVersion.compareTo(V3_5) >= 0) {
            createIndexForNeo4j3(label, property);
        } else{
            throw new DatabaseException(String.format(
                    "Index creation for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    kernelVersion
            ));
        }
    }

    public void createUniqueConstraint(String name, String label, String property) throws DatabaseException {
        if (kernelVersion.compareTo(V5_0) >= 0) {
            createUniqueConstraintForNeo4j5(name, label, property);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            createUniqueConstraintForNeo4j4(name, label, property);
        } else if (kernelVersion.compareTo(V3_5) >= 0) {
            createUniqueConstraintForNeo4j3(label, property);
        } else {
            throw new DatabaseException(String.format(
                    "Unique constraint creation for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    kernelVersion
            ));
        }
    }

    public void createNodeKeyConstraint(String name, String label, String firstProperty, String... rest) throws DatabaseException {
        if (!this.isEnterprise()) {
            return;
        }
        String[] properties = Arrays.prepend(firstProperty, rest, String.class);
        if (kernelVersion.compareTo(V5_0) >= 0) {
            createNodeKeyConstraintForNeo4j5(name, label, properties);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            createNodeKeyConstraintForNeo4j4(name, label, properties);
        } else if (kernelVersion.compareTo(V3_5) >= 0) {
            createNodeKeyConstraintForNeo4j3(label, properties);
        } else{
            throw new DatabaseException(String.format(
                    "Node key constraint creation for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    String.join(", ", properties),
                    kernelVersion
            ));
        }
    }

    public void dropIndex(String name, String label, String property) throws DatabaseException {
        if (kernelVersion.compareTo(V5_0) >= 0) {
            dropIndexForNeo4j5(name);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            dropIndexForNeo4j4(name);
        } else if (kernelVersion.compareTo(V3_5) >= 0) {
            dropIndexForNeo4j3(label, property);
        } else {
            throw new DatabaseException(String.format(
                    "Unique constraint removal for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    kernelVersion
            ));
        }
    }

    public void dropUniqueConstraint(String name, String label, String property) throws DatabaseException {
        if (kernelVersion.compareTo(V5_0) >= 0) {
            dropConstraintForNeo4j5(name);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            dropConstraintForNeo4j4(name);
        } else if (kernelVersion.compareTo(V3_5) >= 0) {
            dropUniqueConstraintForNeo4j3(label, property);
        } else {
            throw new DatabaseException(String.format(
                    "Unique constraint removal for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    property,
                    kernelVersion
            ));
        }
    }

    public void dropNodeKeyConstraint(String name, String label, String firstProperty, String... rest) throws DatabaseException {
        if (!this.isEnterprise()) {
            return;
        }
        String[] properties = Arrays.prepend(firstProperty, rest, String.class);
        if (kernelVersion.compareTo(V5_0) >= 0) {
            dropConstraintForNeo4j5(name);
        } else if (kernelVersion.compareTo(V4_0) >= 0) {
            dropConstraintForNeo4j4(name);
        } else if (kernelVersion.compareTo(V3_5) >= 0) {
            dropNodeKeyConstraintForNeo4j3(label, properties);
        } else {
            throw new DatabaseException(String.format(
                    "Node key constraint removal for (n:%s {%s}) aborted: Neo4j version %s is not supported",
                    label,
                    String.join(", ", properties),
                    kernelVersion
            ));
        }
    }

    public void execute(SqlStatement statement) throws LiquibaseException {
        jdbcExecutor().execute(statement);
    }

    public List<Map<String, ?>> run(SqlStatement statement) throws LiquibaseException {
        return jdbcExecutor().queryForList(statement);
    }

    public boolean supportsCallInTransactions() {
        return kernelVersion.compareTo(KernelVersion.V4_4) >= 0;
    }

    public KernelVersion getKernelVersion() {
        return kernelVersion;
    }

    public boolean isEnterprise() {
        return neo4jEdition.equals("enterprise");
    }

    private void initializeServerAttributes() {
        Map<String, ?> components = readComponents();
        this.kernelVersion = KernelVersion.parse((String) components.get("version"));
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
        // `CREATE CONSTRAINT IF NOT EXISTS` is only available with Neo4j 4.1.3+
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

    // before 4.x, indices cannot be given names

    private void createIndexForNeo4j3(String label, String property) {
        onThrow(this::rollbackIfIndexExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE INDEX ON :`%s`(%s)", label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
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

    private void createIndexForNeo4j4(String name, String label, String property) {
        // `CREATE INDEX IF NOT EXISTS` is only available with Neo4j 4.1.3+
        onThrow(this::rollbackIfIndexExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE INDEX `%s` FOR (n:`%s`) ON (n.`%s`)", name, label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void createNodeKeyConstraintForNeo4j4(String name, String label, String[] properties) {
        // `CREATE CONSTRAINT IF NOT EXISTS` is only available with Neo4j 4.1.3+
        String list = stream(properties).map(p -> String.format("n.`%s`", p)).collect(Collectors.joining(", ", "(", ")"));
        onThrow(this::rollbackIfConstraintExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("CREATE CONSTRAINT `%s` ON (n:`%s`) ASSERT %s IS NODE KEY", name, label, list));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

    private void createIndexForNeo4j5(String name, String label, String property) {
        try {
            RawSqlStatement sql = new RawSqlStatement(String.format("CREATE INDEX `%s` IF NOT EXISTS FOR (n:`%s`) ON (n.`%s`)", name, label, property));
            this.execute(sql);
            this.commit();
        } catch (LiquibaseException e) {
            rollbackOnError(e);
        }
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

    private void dropIndexForNeo4j3(String label, String property) {
        onThrow(this::rollbackIfNoSuchIndexExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("DROP INDEX ON :`%s`(`%s`)", label, property));
                    this.execute(sql);
                    this.commit();
                }
        );
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

    private void dropIndexForNeo4j4(String name) {
        // `DROP INDEX IF EXISTS` is only available with Neo4j 4.1.3+
        onThrow(this::rollbackIfNoSuchIndexExists,
                () -> {
                    RawSqlStatement sql = new RawSqlStatement(String.format("DROP INDEX `%s`", name));
                    this.execute(sql);
                    this.commit();
                }
        );
    }

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
        // `DROP CONSTRAINT IF EXISTS` is only available with Neo4j 4.1.3+
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

    private void dropIndexForNeo4j5(String name) {
        try {
            RawSqlStatement sql = new RawSqlStatement(String.format("DROP INDEX `%s` IF EXISTS", name));
            this.execute(sql);
            this.commit();
        } catch (LiquibaseException e) {
            rollbackOnError(e);
        }
    }

    private Executor jdbcExecutor() {
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this);
    }

    private void rollbackIfIndexExists(Exception ex) throws DatabaseException {
        if (!INDEX_ALREADY_EXISTS_ERROR.test(ex)) {
            return;
        }
        this.rollback();
    }

    private void rollbackIfConstraintExists(Exception ex) throws DatabaseException {
        if (!CONSTRAINT_ALREADY_EXISTS_ERROR.test(ex)) {
            return;
        }
        this.rollback();
    }

    private void rollbackIfNoSuchIndexExists(Exception ex) throws DatabaseException {
        if (!NO_SUCH_INDEX_ERROR.test(ex)) {
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
