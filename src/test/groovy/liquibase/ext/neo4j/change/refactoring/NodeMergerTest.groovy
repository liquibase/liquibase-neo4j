package liquibase.ext.neo4j.change.refactoring

import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.exception.LiquibaseException
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

    def "generates statements to merge properties of matching nodes based on each property's policy"(Pattern propertyMatcher, PropertyMergeStrategy strategy, Object result) {
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

    def "fails to generate statements if mergeable properties do not have a policy"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia'}), (:Person {name: 'Zouheir'})")
        def unusedPolicy = PropertyMergePolicy.of(Pattern.compile("nom"), PropertyMergeStrategy.KEEP_ALL)

        when:
        nodeMerger.merge(MergePattern.of("(p:Person)", "p"), [unusedPolicy])

        then:
        def exc = thrown(LiquibaseException)
        exc.message == "could not find merge policy for node property name"
    }

    def "generates statements to merge incoming relationships"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia', age: 22})<-[:MAINTAINED_BY]-(:Project {name: 'Secret'}), (:Person {name: 'Zouheir'})<-[:FOUNDED_BY {year: 2012}]-(:Conference {name: 'Devoxx France'})")
        def pattern = MergePattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of(Pattern.compile("name"), PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(Pattern.compile(".*"), PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)
WITH p {.*} AS person, [ (p)<-[incoming]-() | incoming {.*, type: type(incoming)} ] AS allIncoming
UNWIND allIncoming AS incoming
WITH person, incoming
ORDER BY incoming['type'] ASC
RETURN person AS p, collect(incoming) AS allIncoming
""")
        row["p"]["name"] == "Zouheir"
        row["p"]["age"] == 22
        row["allIncoming"] == [
                [type: "FOUNDED_BY", year: 2012],
                [type: "MAINTAINED_BY"],
        ]
    }

    def "generates statements to merge outgoing relationships"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia', age: 22})-[:MAINTAINS]->(:Project {name: 'Secret'}), (:Person {name: 'Zouheir'})-[:FOUNDED {year: 2012}]->(:Conference {name: 'Devoxx France'})")
        def pattern = MergePattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of(Pattern.compile("name"), PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(Pattern.compile(".*"), PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)
WITH p {.*} AS person, [ (p)-[outgoing]->() | outgoing {.*, type: type(outgoing)} ] AS allOutgoing
UNWIND allOutgoing AS outgoing
WITH person, outgoing
ORDER BY outgoing['type'] ASC
RETURN person AS p, collect(outgoing) AS allOutgoing
""")
        row["p"]["name"] == "Zouheir"
        row["p"]["age"] == 22
        row["allOutgoing"] == [
                [type: "FOUNDED", year: 2012],
                [type: "MAINTAINS"],
        ]
    }

    def "generates statements that preserve existing self-relationships"() {
        given:
        queryRunner.run("CREATE (anastasia:Person {name: 'Anastasia', age: 22})-[:IS {obviously: true}]->(anastasia), (zouheir:Person {name: 'Zouheir'})-[:IS_SAME_AS {evidently: true}]->(zouheir)")
        def pattern = MergePattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of(Pattern.compile("name"), PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(Pattern.compile(".*"), PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)-[r]->(p)
WITH r {.*, type: type(r)} AS rel
ORDER BY rel['type'] ASC
RETURN collect(rel) AS rels
""")
        row["rels"] == [
                [type: "IS", obviously: true],
                [type: "IS_SAME_AS", evidently: true],
        ]
    }

    def "generates statements that yields self-relationships"() {
        given:
        queryRunner.run("CREATE (anastasia:Person {name: 'Anastasia', age: 22})-[:FOLLOWS_1 {direction: 'a to z'}]->(zouheir:Person {name: 'Zouheir'})-[:FOLLOWS_2 {direction: 'z to a'}]->(anastasia)")
        def pattern = MergePattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of(Pattern.compile("name"), PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(Pattern.compile(".*"), PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)-[r]->(p)
WITH r {.*, type: type(r)} AS rel
ORDER BY rel['type'] ASC
RETURN collect(rel) AS rels
""")
        row["rels"] == [
                [type: "FOLLOWS_1", direction: "a to z"],
                [type: "FOLLOWS_2", direction: "z to a"],
        ]
    }
}
