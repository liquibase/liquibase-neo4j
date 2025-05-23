package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class InsertIT extends Neo4jContainerSpec {

    def "runs inserts"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "e2e/insert/changeLog.${format}".toString())
                .setOutput(System.out)
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
        //  regression in core@4.32.0: date time with zone offsets are converted to the corresponding local datetime with java.sql.Timestamp
//        props["zoned_date_time"] == ZonedDateTime.of(LocalDateTime.of(2020, 7, 12, 22, 23, 24), ZoneOffset.ofHours(2))
        props["polite"] == true
        props["picture"] == Base64.getDecoder().decode("DLxmEfVUC9CAmjiNyVphWw==")
        props["bio"].startsWith("Lorem ipsum")

        where:
        format << ["json", "xml", "yaml"]
    }
}
