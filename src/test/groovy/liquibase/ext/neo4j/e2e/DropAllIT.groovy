package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.DropAllCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class DropAllIT extends Neo4jContainerSpec {

    def "drops all data but keeps Liquibase history"() {
        given:
        def command = new CommandScope(DropAllCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)

        when:
        command.execute()

        then:
        def result = queryRunner.getSingleRow("""
            MATCH (n) WHERE none(label IN labels(n) WHERE label STARTS WITH '__Liquibase')
            RETURN count(n) AS count
        """)
        result["count"] == 0L
    }
}
