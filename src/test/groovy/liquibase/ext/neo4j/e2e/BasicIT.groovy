package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.UpdateSqlCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class BasicIT extends Neo4jContainerSpec {

    def "dry-runs migrations"() {
        given:
        def buffer = new ByteArrayOutputStream()
        System.out = new PrintStream(buffer)
        def command = new CommandScope(UpdateSqlCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/basic/changeLog.${format}".toString())
                .setOutput(buffer)
        command.execute()

        expect:
        def output = buffer.toString()
        output.contains("CREATE (:Movie {title: 'My Life', genre: 'Comedy'})")

        where:
        format << ["json", "sql", "xml", "yaml"]
    }

    def "runs migrations"() {
        given:
        run(format)

        expect:
        verifyRun()

        where:
        format << ["json", "sql", "xml", "yaml"]
    }

    def "runs migrations twice without effect"() {
        given:
        2.times {
            run(format)
        }

        expect:
        verifyRun()

        where:
        format << ["json", "sql", "xml", "yaml"]
    }

    // TODO: add tests for labels, contexts, tagging and rollback

    private void run(String format) {
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(UpdateSqlCommandStep.CHANGELOG_FILE_ARG, "/e2e/basic/changeLog.${format}".toString())
                .setOutput(System.out)
        command.execute()
    }

    private void verifyRun() {
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

        assert row == [
                labels    : ["Movie"],
                properties: [[k: "genre", v: "Comedy"], [k: "title", v: "My Life"]],
                rel_count : 0
        ]
    }
}
