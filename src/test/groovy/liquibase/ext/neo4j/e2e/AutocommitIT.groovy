package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.supportsCypherCallInTransactions

class AutocommitIT extends Neo4jContainerSpec {

    @Requires({ supportsCypherCallInTransactions() })
    def "runs autocommit transaction"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, "/e2e/autocommit/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def row = queryRunner.getSingleRow("""
            MATCH (n)
            WHERE none(label IN labels(n) WHERE label STARTS WITH "__Liquibase")
            UNWIND labels(n) AS label 
            WITH n, label
            ORDER BY label ASC
            WITH n, collect(label) AS labels
            UNWIND keys(n) AS key
            WITH n, labels, {k: key, v: n[key]} AS property
            ORDER BY labels ASC, key ASC, n[key] ASC
            OPTIONAL MATCH (n)-[r]-()
            RETURN labels, collect(property) AS properties, count(r) AS rel_count
        """)

        row == [
                labels    : ["Movie"],
                properties: [[k: "genre", v: "Comedy"], [k: "title", v: "My Life"]],
                rel_count : 0
        ]

        where:
        format << ["json", "sql", "xml", "yaml"]
    }
}
