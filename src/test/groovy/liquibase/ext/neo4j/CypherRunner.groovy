package liquibase.ext.neo4j


import liquibase.statement.SqlStatement
import liquibase.statement.core.RawParameterizedSqlStatement
import liquibase.statement.core.RawSqlStatement
import org.neo4j.driver.Driver

import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.IntStream

import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_0_0
import static liquibase.ext.neo4j.database.KernelVersion.V4_3_0
import static liquibase.ext.neo4j.database.KernelVersion.V5_0_0

class CypherRunner implements AutoCloseable {

    private final Driver driver

    CypherRunner(Driver driver) {
        this.driver = driver
    }

    void createIndex(String name, String label, String property) {
        ignoring(exceptionMessageContaining("index already exists"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "CREATE INDEX $name IF NOT EXISTS FOR (n:$label) ON (n.$property)" : neo4jVersion >= V4_0_0 ?
                    "CREATE INDEX $name FOR (n:$label) ON (n.$property)" :
                    "CREATE INDEX ON :$label($property)"
            this.run(query)
        })
    }

    void createUniqueConstraint(String name, String label, String property) {
        ignoring(exceptionMessageContaining("constraint already exists"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "CREATE CONSTRAINT $name IF NOT EXISTS FOR (c:$label) REQUIRE c.$property IS UNIQUE" : neo4jVersion >= V4_0_0 ?
                    "CREATE CONSTRAINT $name ON (c:$label) ASSERT c.$property IS UNIQUE" :
                    "CREATE CONSTRAINT ON (c:$label) ASSERT c.$property IS UNIQUE"
            this.run(query)
        })
    }

    void createNodeKeyConstraint(String name, String label, String... properties) {
        ignoring(exceptionMessageContaining("constraint already exists"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "CREATE CONSTRAINT $name IF NOT EXISTS FOR (c:$label) REQUIRE (${properties.collect { "c.`$it`" }.join(", ")}) IS NODE KEY" : neo4jVersion >= V4_0_0 ?
                    "CREATE CONSTRAINT $name ON (c:$label) ASSERT (${properties.collect { "c.`$it`" }.join(", ")}) IS NODE KEY" :
                    "CREATE CONSTRAINT ON (c:$label) ASSERT (${properties.collect { "c.`$it`" }.join(", ")}) IS NODE KEY"
            this.run(query)
        })
    }

    List<String> listExistingConstraints() {
        if (neo4jVersion() >= V4_3_0) {
            def descriptions = (String[]) this.getSingleRow(
                    """
                    | SHOW CONSTRAINTS YIELD name, labelsOrTypes
                    | RETURN collect(name + ":" + reduce(str = "", l IN labelsOrTypes | str+l+",")) AS descriptions
                    """.stripMargin())["descriptions"]
            return Arrays.asList(descriptions)
        }
        // names are not available before Neo4j 4.x
        def descriptions = (String[]) this.getSingleRow("CALL db.constraints() YIELD description RETURN collect(description) AS descriptions")["descriptions"]
        return Arrays.asList(descriptions)
    }

    List<String> listExistingIndices() {
        if (neo4jVersion() >= V4_3_0) {
            def descriptions = (String[]) this.getSingleRow(
                    """
                    | SHOW INDEXES YIELD name, labelsOrTypes
                    | RETURN collect(name + ":" + reduce(str = "", l IN labelsOrTypes | str+l+",")) AS descriptions
                    """.stripMargin())["descriptions"]
            return Arrays.asList(descriptions)
        }
        if (neo4jVersion() >= V4_0_0) {
            def descriptions = (String[]) this.getSingleRow(
                    """
                    | CALL db.indexes() YIELD name, labelsOrTypes
                    | RETURN collect(name + ":" + reduce(str = "", l IN labelsOrTypes | str+l+",")) AS descriptions
                    """.stripMargin())["descriptions"]
            return Arrays.asList(descriptions)
        }
        // names are not available before Neo4j 4.x
        def descriptions = (String[]) this.getSingleRow("CALL db.indexes() YIELD description RETURN collect(description) AS descriptions")["descriptions"]
        return Arrays.asList(descriptions)
    }

    void dropIndex(String name, String label, String property) {
        ignoring(exceptionMessageContaining("no such index"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "DROP INDEX $name IF EXISTS" : neo4jVersion >= V4_0_0 ?
                    "DROP INDEX $name" :
                    "DROP INDEX ON :$label(`$property`)"
            this.run(query)
        })
    }

    void dropUniqueConstraint(String name, String label, String property) {
        ignoring(exceptionMessageContaining("no such constraint"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "DROP CONSTRAINT $name IF EXISTS" : neo4jVersion>= V4_0_0 ?
                    "DROP CONSTRAINT $name" :
                    "DROP CONSTRAINT ON (c:$label) ASSERT c.$property IS UNIQUE"
            this.run(query)
        })
    }

    void dropNodeKeyConstraint(String name, String label, String... properties) {
        ignoring(exceptionMessageContaining("no such constraint"), {
            def neo4jVersion = neo4jVersion()
            def query = neo4jVersion >= V5_0_0 ?
                    "DROP CONSTRAINT $name IF EXISTS" : neo4jVersion>= V4_0_0 ?
                    "DROP CONSTRAINT $name" :
                    "DROP CONSTRAINT ON (c:$label) ASSERT (${properties.collect { "c.`$it`" }.join(", ")}) IS NODE KEY"
            this.run(query)
        })
    }

    Map<String, Object> getSingleRow(String query) {
        driver.session().withCloseable { session ->
            session.executeRead({ tx ->
                tx.run(query).single().asMap()
            })
        }
    }

    List<Map<String, Object>> getRows(String query) {
        driver.session().withCloseable { session ->
            session.executeRead({ tx ->
                tx.run(query).list({ record -> record.asMap() })
            })
        }
    }

    void run(SqlStatement statement) {
        if (statement instanceof RawSqlStatement) {
            run(statement.sql)
            return
        }
        if (statement instanceof RawParameterizedSqlStatement) {
            def parameters = statement.parameters
            Map<String, Object> indexedMap = IntStream.range(0, parameters.size())
                    .boxed()
                    .collect(Collectors.toMap((Integer index) -> (index+1).toString(), (Integer index) -> parameters.get(index)))
            run(statement.sql, indexedMap)
            return
        }
        throw new IllegalArgumentException("unsupported type of statement: ${statement.getClass()}")
    }

    void run(String query) {
        run(query, new HashMap<String, Object>(0))
    }

    void run(String query, Map<String, Object> params) {
        driver.session().withCloseable { session ->
            session.executeWrite({ tx ->
                return tx.run(query, params).consume()
            })
        }
    }

    @Override
    void close() throws Exception {
        this.driver.close()
    }

    private static Predicate<Exception> exceptionMessageContaining(String message) {
        return { e -> e.getMessage().toLowerCase(Locale.ENGLISH).contains(message) }
    }

    private static void ignoring(Predicate<Exception> predicate, Closure closure) {
        try {
            closure.run()
        } catch (e) {
            if (!predicate.test(e)) {
                throw e
            }
        }
    }
}
