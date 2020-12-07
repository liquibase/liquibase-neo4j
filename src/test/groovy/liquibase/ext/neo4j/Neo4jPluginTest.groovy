package liquibase.ext.neo4j

import liquibase.integration.commandline.Main
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.time.ZoneId
import java.util.logging.LogManager

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CHANGE_SET_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CONTEXT_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.LABEL_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.TAG_CONSTRAINT_NAME

class Neo4jPluginTest extends Specification {

    static {
        LogManager.getLogManager().reset()
    }

    private static final String PASSWORD = "s3cr3t"

    private static final TIMEZONE = ZoneId.of("Europe/Paris")

    @Shared
    GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(PASSWORD, TIMEZONE)

    @Shared
    CypherRunner queryRunner

    private PrintStream stdout

    private PrintStream stderr

    def setupSpec() {
        neo4jContainer.start()
        queryRunner = new CypherRunner(
                GraphDatabase.driver(neo4jContainer.getBoltUrl(),
                        AuthTokens.basic("neo4j", PASSWORD)),
                neo4jVersion())
    }

    def cleanupSpec() {
        queryRunner.close()
        neo4jContainer.stop()
    }

    def setup() {
        stdout = System.out
        stderr = System.err
        System.err = mute()
    }

    def cleanup() {
        queryRunner.run("MATCH (n) DETACH DELETE n")
        queryRunner.dropUniqueConstraint(TAG_CONSTRAINT_NAME, "__LiquibaseTag", "tag")
        queryRunner.dropUniqueConstraint(LABEL_CONSTRAINT_NAME, "__LiquibaseLabel", "label")
        queryRunner.dropUniqueConstraint(CONTEXT_CONSTRAINT_NAME, "__LiquibaseContext", "context")
        if (enterpriseEdition()) {
            queryRunner.dropNodeKeyConstraint(CHANGE_SET_CONSTRAINT_NAME, "__LiquibaseChangeSet", "id", "author", "changeLog")
        }
        System.out = stdout
        System.err = stderr
    }

    def "dry-runs migrations"() {
        given:
        def buffer = new ByteArrayOutputStream()
        System.out = new PrintStream(buffer)
        String[] arguments = [
                "--url", "jdbc:neo4j:${neo4jContainer.getBoltUrl()}",
                "--username", "neo4j",
                "--password", PASSWORD,
                "--changeLogFile", "classpath:/changelog.xml",
                "updateSQL"
        ].toArray()

        when:
        Main.run(arguments)

        then:
        def output = buffer.toString()
        output.contains("MERGE (:Movie {title: 'My Life'})")
        output.contains("MATCH (m:Movie) WITH COUNT(m) AS count MERGE (c:Count) SET c.value = count")
        output.contains("""
MERGE (m:Movie {title: 'My Life'})
            MERGE (a:Person {name: 'Myself'})
            MERGE (a)-[:ACTED_IN]->(m)
""".trim())
        output.contains("""
MERGE (m:Movie {title: 'My Life'})
            MERGE (a:Person {name: 'Hater'})
            MERGE (a)-[:RATED {rating: 0}]->(m)
""".trim())
    }

    def "runs migrations"() {
        given:
        System.out = mute()
        String[] arguments = [
                "--url", "jdbc:neo4j:${neo4jContainer.getBoltUrl()}",
                "--username", "neo4j",
                "--password", PASSWORD,
                "--changeLogFile", "classpath:/changelog.xml",
                "update"
        ].toArray()

        when:
        Main.run(arguments)

        then:
        def rows = queryRunner.getRows("""
            MATCH (m)
            WHERE NONE(label IN LABELS(m) WHERE label STARTS WITH "__Liquibase")
            OPTIONAL MATCH (m)-[r]->()
            WITH m, r
            ORDER BY LABELS(m)[0] ASC, TYPE(r) ASC
            WITH m AS node, COLLECT(r {type: TYPE(r), properties: PROPERTIES(r)}) AS outgoing_relationships
            RETURN LABELS(node) AS labels, PROPERTIES(node) AS properties, outgoing_relationships
        """)
        rows.size() == 4
        rows[0] == [labels: ["Count"], properties: [value: 1], outgoing_relationships: []]
        rows[1] == [labels: ["Movie"], properties: [title: "My Life"], outgoing_relationships: []]
        rows[2] == [labels: ["Person"], properties: [name: "Myself"], outgoing_relationships: [[type: "ACTED_IN", properties: [:]]]]
        rows[3] == [labels: ["Person"], properties: [name: "Hater"], outgoing_relationships: [[type: "RATED", properties: ["rating": 0]]]]
    }


    private static PrintStream mute() {
        new PrintStream(Files.createTempFile("liquibase", "neo4j").toFile())
    }
}
