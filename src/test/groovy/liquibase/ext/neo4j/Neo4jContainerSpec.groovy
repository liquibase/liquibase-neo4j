package liquibase.ext.neo4j

import liquibase.database.Database
import liquibase.database.DatabaseFactory
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.time.ZoneId
import java.util.logging.LogManager

import static liquibase.ext.neo4j.DockerNeo4j.dockerTag
import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CHANGE_SET_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CONTEXT_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.LABEL_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.TAG_CONSTRAINT_NAME

abstract class Neo4jContainerSpec extends Specification {

    static {
        LogManager.getLogManager().reset()
    }

    static final String PASSWORD = "supers3cr3t"

    static final TIMEZONE = ZoneId.of("Europe/Paris")

    @Shared
    Neo4jContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(PASSWORD, TIMEZONE, neo4jImageVersion())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

    @Shared
    CypherRunner queryRunner

    @Shared
    Database database

    private PrintStream stdout

    private PrintStream stderr

    def setupSpec() {
        neo4jContainer.start()
        queryRunner = new CypherRunner(
                GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", PASSWORD)), dockerTag())
        database = DatabaseFactory.instance.openDatabase(
                "jdbc:neo4j:${neo4jContainer.getBoltUrl()}",
                "neo4j",
                PASSWORD,
                null,
                null
        )
    }

    def setup() {
        stdout = System.out
        stderr = System.err
        System.out = tempFilePrintStream()
        System.err = tempFilePrintStream()
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

    def cleanupSpec() {
        database.close()
        queryRunner.close()
        neo4jContainer.stop()
    }

    def jdbcUrl() {
        return "jdbc:neo4j:${neo4jContainer.getBoltUrl()}"
    }

    def authenticationProperties() {
        def properties = new Properties()
        properties.setProperty("user", "neo4j")
        properties.setProperty("password", neo4jContainer.getAdminPassword())
        return properties
    }

    protected neo4jImageVersion() {
        return dockerTag()
    }

    static PrintStream tempFilePrintStream() {
        new PrintStream(Files.createTempFile("liquibase", "neo4j").toFile())
    }
}
