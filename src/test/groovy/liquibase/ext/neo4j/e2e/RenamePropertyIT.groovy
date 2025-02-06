package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V5_21_0
import static org.junit.jupiter.api.Assumptions.assumeTrue

class RenamePropertyIT extends Neo4jContainerSpec {

    def "runs migrations renaming properties of all entities"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-all.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs migrations renaming properties of nodes only"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-node.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [calendar_date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs migrations renaming properties of relationships only"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-rel.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [calendar_date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        format << ["json", "xml", "yaml"]
    }

    def "runs batched migrations renaming properties of all entities"() {
        given:
        if (concurrent) {
            assumeTrue(neo4jVersion() >= V5_21_0)
        }
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-all-batched${if (concurrent) "-concurrent" else ""}.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        [format, concurrent] << [["json", "xml", "yaml"], [false, true]].combinations()
    }

    def "runs batched migrations renaming properties of nodes only"() {
        given:
        if (concurrent) {
            assumeTrue(neo4jVersion() >= V5_21_0)
        }
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-node-batched${if (concurrent) "-concurrent" else ""}.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [calendar_date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        [format, concurrent] << [["json", "xml", "yaml"], [false, true]].combinations()
    }

    def "runs batched migrations renaming properties of rels only"() {
        given:
        if (concurrent) {
            assumeTrue(neo4jVersion() >= V5_21_0)
        }
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-property/changeLog-rel-batched${if (concurrent) "-concurrent" else ""}.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
            MATCH (s)-[r]->(e)
            WHERE none(label IN labels(s) WHERE label STARTS WITH "__Liquibase")
            RETURN {
                rel_type: type(r),
                rel_props: properties(r), 
                start_labels: labels(s),
                start_props: properties(s), 
                end_labels: labels(e),
                end_props: properties(e)
            } AS result
        """)

        rows["result"] == [
                [
                        rel_type    : "SEEN_BY",
                        rel_props   : [date: 'now'],
                        start_labels: ['Movie'],
                        start_props : [calendar_date: 'today'],
                        end_labels  : ['Person'],
                        end_props   : [:]
                ]
        ]

        where:
        [format, concurrent] << [["json", "xml", "yaml"], [false, true]].combinations()
    }
}
