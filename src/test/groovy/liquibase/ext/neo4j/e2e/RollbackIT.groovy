package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.RollbackCountCommandStep
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class RollbackIT extends Neo4jContainerSpec {

    def "rolls back by count"() {
        given:
        def changeLogFile = "/e2e/rollback/changeLog.${format}".toString()
        runUpdate(changeLogFile)
        def row = queryRunner.getSingleRow("MATCH (m:Movie) RETURN m.genre AS genre")
        assert row["genre"].equals("Comédie")

        def command = new CommandScope(RollbackCountCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogFile)
                .addArgumentValue(RollbackCountCommandStep.COUNT_ARG, 1)
                .setOutput(System.out)
        command.execute()

        expect:
        def result = queryRunner.getSingleRow("MATCH (m:Movie) RETURN m.genre AS genre")
        result["genre"].equals("Comedy")

        where:
        format << ["cypher", "json", "xml", "yaml"]

    }

    private def runUpdate(String changeLogFile) {
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogFile)
                .setOutput(System.out)
        command.execute()
    }
}
