package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class ExtractPropertyIT extends Neo4jContainerSpec {

    def "runs migrations extracting properties"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/extract-property/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (g:Genre)
            RETURN g.genre AS genre
        """)

        rows.size() == 2
        rows[0] == [genre: "Comedy"]
        rows[1] == [genre: "Comedy"]

        where:
        format << ["json", "xml", "yaml"]
    }
}
