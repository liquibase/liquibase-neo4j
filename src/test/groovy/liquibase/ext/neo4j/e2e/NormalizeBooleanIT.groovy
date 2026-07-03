package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class NormalizeBooleanIT extends Neo4jContainerSpec {

    def "normalizes string boolean properties on nodes and relationships"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/normalize-boolean/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def movieCounts = queryRunner.getSingleRow("""
            MATCH (m:Movie)
            RETURN
                count(CASE WHEN m.watched = true THEN 1 END) AS trueCount,
                count(CASE WHEN m.watched = false THEN 1 END) AS falseCount,
                count(CASE WHEN m.watched IS NULL THEN 1 END) AS missingCount
        """)

        movieCounts["trueCount"] == 2
        movieCounts["falseCount"] == 1
        movieCounts["missingCount"] == 1

        def relationship = queryRunner.getSingleRow("""
            MATCH ()-[r:SEEN]->()
            RETURN r.watched AS watched
        """)

        relationship["watched"] == true

        where:
        format << ["json", "xml", "yaml"]
    }
}
