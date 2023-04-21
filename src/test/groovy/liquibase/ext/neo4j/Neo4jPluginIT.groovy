package liquibase.ext.neo4j

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep

import java.nio.file.Files
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CHANGE_SET_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.CONTEXT_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.LABEL_CONSTRAINT_NAME
import static liquibase.ext.neo4j.changelog.Neo4jChangelogHistoryService.TAG_CONSTRAINT_NAME

class Neo4jPluginIT extends Neo4jContainerSpec {

    private PrintStream stdout

    private PrintStream stderr

    def setup() {
        stdout = System.out
        stderr = System.err
        System.err = mute()
    }

    def cleanup() {
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
        def command = new CommandScope(UpdateSqlCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateSqlCommandStep.CHANGELOG_FILE_ARG, "/changelog.xml")
                .setOutput(buffer)

        when:
        command.execute()

        then:
        def output = buffer.toString()
        output.contains("MERGE (:Movie {title: 'My Life', genre: 'Horror'})")
        output.contains("MATCH (m:Movie) WITH count(m) AS count MERGE (c:Count) SET c.value = count")
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
        output.contains("""
MATCH (m:Movie {title: 'My Life'})
MATCH (a:Person {name: 'Hater'})
MATCH (a)-[r:RATED {rating: 0}]->(m) SET r.rating = 5
""".trim())
        if (definesExtraNode(queryRunner)) {
            output.contains("""
CREATE (:SecretMovie {title: 'Neo4j 4.4 EE: A life story'});
""")
        }
        output.contains("CREATE (:Movie {title: 'My Life', genre: 'Horror'});")
        output.contains("UNWIND \$0 AS row CREATE (n:`CsvPerson`) SET n += row;")
        output.contains("""
MATCH (m:Movie) WITH m ORDER BY id(m) ASC WITH m MERGE (_____n_____:`Genre` {`genre`: m.`genre`}) CREATE (m)-[:`HAS_GENRE`]->(_____n_____) REMOVE m.`genre`;
""".trim())
    }

    def "runs migrations"() {
        given:
        def output = mute()
        System.out = output
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, "/changelog.xml")
                .setOutput(output)

        when:
        command.execute()

        then:
        def rows = fixUpProperties(queryRunner.getRows("""
            MATCH (n)
            WHERE none(label IN labels(n) WHERE label STARTS WITH "__Liquibase")
            UNWIND labels(n) AS label 
            WITH n, label
            ORDER BY label ASC
            WITH n, collect(label) AS labels
            UNWIND keys(n) AS key
            WITH n, labels, {k: key, v: n[key]} AS property
            ORDER BY labels ASC, key ASC, n[key] ASC
            WITH n, labels, collect(property) AS properties
            OPTIONAL MATCH (n)-[r]->()
            WITH labels, properties, collect(r {type: type(r), properties: properties(r)}) AS outgoing_relationships
            RETURN labels, properties, outgoing_relationships
        """))

        def hasExtraNodeFromConditionalChangeSet = definesExtraNode(queryRunner)
        rows.size() == (hasExtraNodeFromConditionalChangeSet ? 10 : 9)
        rows[0] == [labels: ["Count"], properties: [value: 1], outgoing_relationships: []]
        rows[1] == [labels       : ["CsvPerson"], properties: [
                "first_name"  : "Andrea",
                "polite"      : true,
                "some_date"   : ZonedDateTime.of(LocalDateTime.of(2020, 7, 12, 22, 23, 24), ZoneOffset.ofHours(2)),
                "uuid"        : "1bc59ddb-8d4d-41d0-9c9a-34e837de5678",
                "wisdom_index": 32L,
        ], outgoing_relationships: []]
        rows[2] == [labels       : ["CsvPerson"], properties: [
                "first_name"  : "Florent",
                "picture"     : Base64.getDecoder().decode("DLxmEfVUC9CAmjiNyVphWw=="),
                "polite"      : false,
                "some_date"   : LocalDate.of(2022, 12, 25),
                "uuid"        : "8d1208fc-f401-496c-9cb8-483fef121234",
                "wisdom_index": 30.5D,
        ], outgoing_relationships: []]
        rows[3] == [labels       : ["CsvPerson"], properties: [
                "first_name"  : "Nathan",
                "polite"      : true,
                "some_date"   : LocalDateTime.of(2018, 2, 1, 12, 13, 14),
                "uuid"        : "123e4567-e89b-12d3-a456-426614174000",
                "wisdom_index": 34L,
        ], outgoing_relationships: []]
        rows[4] == [labels       : ["CsvPerson"], properties: [
                "first_name"  : "Robert",
                "polite"      : true,
                "some_date"   : LocalTime.of(22, 23, 24),
                "uuid"        : "9986a49a-0cce-4982-b491-b8177fd0ef81",
                "wisdom_index": 36L,
        ], outgoing_relationships: []]
        rows[5] == [labels: ["Genre"], properties: [genre: "Horror"], outgoing_relationships: []]
        rows[6] == [labels: ["Movie"], properties: [title: "My Life"], outgoing_relationships: [[type: "HAS_GENRE", properties: [:]]]]
        rows[7] == [labels: ["Person"], properties: [name: "Hater"], outgoing_relationships: [[type: "RATED", properties: ["rating": 5]]]]
        rows[8] == [labels: ["Person"], properties: [name: "Myself"], outgoing_relationships: [[type: "ACTED_IN", properties: [:]]]]
        if (hasExtraNodeFromConditionalChangeSet) {
            rows[9] == [labels: ["SecretMovie"], properties: [title: "Neo4j 4.4 EE: A life story"], outgoing_relationships: []]
        }
    }

    def "runs inserts"() {
        given:
        def output = mute()
        System.out = output
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, "/insert/changelog.${format}".toString())
                .setOutput(output)

        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (p:Person)
            RETURN properties(p) AS props
        """)
        def props = row["props"]
        props["id"] == "8987212b-a6ff-48a1-901f-8c4b39bd6d9e"
        props["age"] == 30L
        props["first_name"] == "Florent"
        props["last_name"] == "Biville"
        props["local_date"] == LocalDate.of(2022, 12, 25)
        props["local_time"] == LocalTime.of(22, 23, 24)
        props["local_date_time"] == LocalDateTime.of(2018, 2, 1, 12, 13, 14)
        props["zoned_date_time"] == ZonedDateTime.of(LocalDateTime.of(2020, 7, 12, 22, 23, 24), ZoneOffset.ofHours(2))
        props["polite"] == true
        props["picture"] == Base64.getDecoder().decode("DLxmEfVUC9CAmjiNyVphWw==")
        props["bio"].startsWith("Lorem ipsum")

        where:
        format << ["json", "xml", "yaml"]
    }

    private static PrintStream mute() {
        new PrintStream(Files.createTempFile("liquibase", "neo4j").toFile())
    }

    private static boolean definesExtraNode(CypherRunner cypherRunner) {
        def results = cypherRunner.getSingleRow("""
        CALL dbms.components()
        YIELD name, versions, edition
        WHERE name = 'Neo4j Kernel' 
        RETURN versions[0] AS version, edition = "enterprise" AS isEnterprise
        """)
        return results["version"].startsWith("4.4") && results["isEnterprise"] == true
    }

    // TODO: gather all other keys automatically
    private static List<Map<String, Object>> fixUpProperties(List<Map<String, Object>> rows) {
        return rows.collect(row -> {
            def props = row["properties"].collectEntries { [it.k, it.v] }
            return [
                    labels                : row["labels"],
                    outgoing_relationships: row["outgoing_relationships"],
                    properties            : props
            ]
        })
    }
}
