package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.database.KernelVersion

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion

class PreconditionsIT extends Neo4jContainerSpec {

    def "runs migrations depending on Neo4j edition"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/preconditions/neo4jEditionChangeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (n:Neo4j) RETURN n.enterprise AS isEnterprise
        """)

        row["isEnterprise"] == enterpriseEdition()

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs migrations depending on Neo4j version"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/preconditions/neo4jVersionChangeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (n:Neo4j) RETURN n.neo4j44 AS isNeo4j44
        """)


        def version = neo4jVersion()
        row["isNeo4j44"] == (version >= KernelVersion.V4_4_0 && version.major() == 4 && version.minor() == 4)

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs migrations depending on Cypher result"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/preconditions/cypherCheckChangeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (n:Neo4j) RETURN count(n) AS count
        """)

        row["count"] == 1

        where:
        format << ["json", "xml", "yaml"]
    }
}
