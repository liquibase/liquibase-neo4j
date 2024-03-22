package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.DropAllCommandStep
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class DropAllIT extends Neo4jContainerSpec {

    def "drops all data, indices and constraints"() {
        given:
        // init history and insert some data
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateSqlCommandStep.CHANGELOG_FILE_ARG, "/e2e/basic/changeLog.xml")
                .setOutput(System.out)
        command.execute()

        and:
        command = new CommandScope(DropAllCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)

        when:
        command.execute()

        then:
        queryRunner.listExistingIndices().isEmpty()
        queryRunner.listExistingConstraints().isEmpty()
        def result = queryRunner.getSingleRow("MATCH (n) RETURN count(n) AS count")
        result["count"] == 0L
    }
}
