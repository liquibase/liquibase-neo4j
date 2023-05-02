package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.CypherRunner
import liquibase.ext.neo4j.Neo4jContainerSpec

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

class ExtractPropertyIT extends Neo4jContainerSpec {

    def "runs migrations extracting properties"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, "/e2e/extract-property/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (m:Movie)-[:HAS_GENRE]->(g:Genre)
            RETURN m{.*, genre: g.genre} AS movie
        """)

        row == [movie: [genre: "Comedy", title: "My Life"]]

        where:
        format << ["json", "xml", "yaml"]
    }
}
