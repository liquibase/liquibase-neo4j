package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class RenameLabelIT extends Neo4jContainerSpec {

    def "runs migrations renaming labels"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/rename-label/changeLog-simple.${format}".toString())
                .setOutput(System.out)
        command.execute()

        expect:
        def rows = queryRunner.getRows("""
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

        rows == [
            [
                labels    : ["Book"],
                properties: [[k: "genre", v: "Autobiography"], [k: "title", v: "My Life"]],
                rel_count : 0
            ],
            [
                labels    : ["Film"],
                properties: [[k: "genre", v: "Comedy"], [k: "title", v: "My Life"]],
                rel_count : 0
            ]
        ]

        where:
        format << ["json", "xml", "yaml"]
    }
}
