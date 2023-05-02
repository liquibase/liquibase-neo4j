package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

class LoadDataIT extends Neo4jContainerSpec {

    def "runs migrations loading seed data"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, "/e2e/load-data/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
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
            OPTIONAL MATCH (n)-[r]-()
            WITH n, labels, properties, count(r) AS rel_count
            RETURN labels, properties, rel_count
        """)

        rows.size() == 4
        rows[0] == [labels: ["CsvPerson"], properties: [
                [k: "first_name", v: "Andrea"],
                [k: "polite", v: true],
                [k: "some_date", v: ZonedDateTime.of(LocalDateTime.of(2020, 7, 12, 22, 23, 24), ZoneOffset.ofHours(2))],
                [k: "uuid", v: "1bc59ddb-8d4d-41d0-9c9a-34e837de5678"],
                [k: "wisdom_index", v: 32L],
        ], rel_count      : 0]
        rows[1] == [labels: ["CsvPerson"], properties: [
                [k: "first_name", v: "Florent"],
                [k: "picture", v: Base64.getDecoder().decode("DLxmEfVUC9CAmjiNyVphWw==")],
                [k: "polite", v: false],
                [k: "some_date", v: LocalDate.of(2022, 12, 25)],
                [k: "uuid", v: "8d1208fc-f401-496c-9cb8-483fef121234"],
                [k: "wisdom_index", v: 30.5D],
        ], rel_count      : 0]
        rows[2] == [labels: ["CsvPerson"], properties: [
                [k: "first_name", v: "Nathan"],
                [k: "polite", v: true],
                [k: "some_date", v: LocalDateTime.of(2018, 2, 1, 12, 13, 14)],
                [k: "uuid", v: "123e4567-e89b-12d3-a456-426614174000"],
                [k: "wisdom_index", v: 34L],
        ], rel_count      : 0]
        rows[3] == [labels: ["CsvPerson"], properties: [
                [k: "first_name", v: "Robert"],
                [k: "polite", v: true],
                [k: "some_date", v: LocalTime.of(22, 23, 24)],
                [k: "uuid", v: "9986a49a-0cce-4982-b491-b8177fd0ef81"],
                [k: "wisdom_index", v: 36L],
        ], rel_count      : 0]

        where:
        format << ["json", "xml", "yaml"]
    }
}
