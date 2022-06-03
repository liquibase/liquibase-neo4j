package liquibase.ext.neo4j.change.refactoring

import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.ext.neo4j.CypherRunner
import liquibase.ext.neo4j.DockerNeo4j
import liquibase.ext.neo4j.database.Neo4jDatabase
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZoneId
import java.util.logging.LogManager
import java.util.regex.Pattern

import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion

class NodeMergerTest extends Specification {

    static {
        LogManager.getLogManager().reset()
    }

    private static final String PASSWORD = "s3cr3t"

    static final TIMEZONE = ZoneId.of("Europe/Paris")

    @Shared
    GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(PASSWORD, TIMEZONE)

    @Shared
    CypherRunner queryRunner

    Database database

    NodeMerger nodeMerger

    def setupSpec() {
        neo4jContainer.start()
        queryRunner = new CypherRunner(
                GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", PASSWORD)),
                neo4jVersion())
    }

    def cleanupSpec() {
        queryRunner.close()
        neo4jContainer.stop()
    }

    def setup() {
        database = DatabaseFactory.instance.openDatabase(
                "jdbc:neo4j:${neo4jContainer.getBoltUrl()}",
                "neo4j",
                PASSWORD,
                null,
                null
        )
        nodeMerger = new NodeMerger(database as Neo4jDatabase)
    }

    def cleanup() {
        queryRunner.run("MATCH (n) DETACH DELETE n")
    }

    def "generates statements to merge labels of the matching disconnected nodes"() {
        given:
        queryRunner.run("CREATE (:Label1:Label3), (:Label2:`Label Oops`), (:Label1:`Label Oops`)")
        def pattern = MergePattern.of("(p)", "p")

        when:
        def statements = nodeMerger.merge(pattern, [])

        and:
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (n) UNWIND labels(n) AS label
            WITH label ORDER BY label ASC
            RETURN collect(label) AS labels
        """.stripIndent())
        row["labels"] == ["Label Oops", "Label1", "Label2", "Label3"]
    }

    def "does not generate any statement when less than 2 nodes are matching"(String graphInit, String fragment, String outputVariable) {
        given:
        if (graphInit != "") {
            queryRunner.run(graphInit)
        }

        expect:
        def statements = nodeMerger.merge(MergePattern.of(fragment, outputVariable), [])
        statements.length == 0

        where:
        graphInit               | fragment  | outputVariable
        ""                      | "(n)"     | "n"
        "CREATE (:Foo)"         | "(n:Bar)" | "n"
        "CREATE (:Foo)"         | "(n)"     | "n"
        "CREATE (:Foo)"         | "(n:Foo)" | "n"
        "CREATE (:Foo), (:Bar)" | "(n:Foo)" | "n"
        "CREATE (:Foo), (:Bar)" | "(n:Bar)" | "n"
    }

    def "merges properties of matching nodes based on each property's policy"(Pattern propertyMatcher, PropertyMergeStrategy strategy, Object result) {
        given:
        queryRunner.run("CREATE (:Person), (:Person {name: 'Anastasia'}), (:Unmatched), (:Person {name: 'Zouheir'}), (:Person)")
        def pattern = MergePattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        and:
        def statements = nodeMerger.merge(pattern, [PropertyMergePolicy.of(propertyMatcher, strategy)])
        statements.each queryRunner::run

        expect:
        def row = queryRunner.getSingleRow("MATCH (n:Person) RETURN n {.*}")
        row["n"]["name"] == result

        where:
        propertyMatcher         | strategy                         | result
        Pattern.compile("name") | PropertyMergeStrategy.KEEP_ALL   | ["Anastasia", "Zouheir"]
        Pattern.compile("name") | PropertyMergeStrategy.KEEP_FIRST | "Anastasia"
        Pattern.compile("name") | PropertyMergeStrategy.KEEP_LAST  | "Zouheir"
        Pattern.compile(".*")   | PropertyMergeStrategy.KEEP_ALL   | ["Anastasia", "Zouheir"]
        Pattern.compile(".*")   | PropertyMergeStrategy.KEEP_FIRST | "Anastasia"
        Pattern.compile(".*")   | PropertyMergeStrategy.KEEP_LAST  | "Zouheir"
    }
}
