package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class InvertDirectionIT extends Neo4jContainerSpec {

    def "runs migrations inverting direction"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/invert-direction/changeLog-simple.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            WITH type(r) AS type, properties(r) AS properties, labels(s) AS start_labels, labels(e) AS end_labels
            ORDER BY type, head(start_labels) ASC, head(end_labels) ASC
            RETURN type, properties, start_labels, end_labels
        """)

        rows == [
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'yesterday'],
                        start_labels: ['Movie'],
                        end_labels  : ['Dog']
                ],
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'now'],
                        start_labels: ['Movie'],
                        end_labels  : ['Person']
                ],
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs batched migrations inverting direction"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/invert-direction/changeLog-simple-batched.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            WITH type(r) AS type, properties(r) AS properties, labels(s) AS start_labels, labels(e) AS end_labels
            ORDER BY type, head(start_labels) ASC, head(end_labels) ASC
            RETURN type, properties, start_labels, end_labels
        """)

        rows == [
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'yesterday'],
                        start_labels: ['Movie'],
                        end_labels  : ['Dog']
                ],
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'now'],
                        start_labels: ['Movie'],
                        end_labels  : ['Person']
                ],
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs migrations inverting direction of matching relationships"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/invert-direction/changeLog-pattern.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN type(r) AS type, properties(r) AS properties, labels(s) AS start_labels, labels(e) AS end_labels
            ORDER BY type ASC, head(start_labels) ASC, head(end_labels) ASC
        """)

        rows == [
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'yesterday'],
                        start_labels: ['Movie'],
                        end_labels  : ['Dog']
                ],
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'now'],
                        start_labels: ['Movie'],
                        end_labels  : ['Person']
                ],
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs batched migrations inverting direction of matching relationships"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/invert-direction/changeLog-pattern-batched.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN type(r) AS type, properties(r) AS properties, labels(s) AS start_labels, labels(e) AS end_labels
            ORDER BY type ASC, head(start_labels) ASC, head(end_labels) ASC
        """)

        rows == [
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'yesterday'],
                        start_labels: ['Movie'],
                        end_labels  : ['Dog']
                ],
                [
                        type        : "VIEWED_BY",
                        properties  : [date: 'now'],
                        start_labels: ['Movie'],
                        end_labels  : ['Person']
                ],
        ]

        where:
        format << ["json", "xml", "yaml"]
    }
}
