package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class FilteringByLabelIT extends Neo4jContainerSpec {

    def "filters by label"() {
        given:
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/filtering-by-label/changeLog.${format}".toString())
                .addArgumentValue(UpdateCommandStep.LABEL_FILTER_ARG, "mandatory")
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
        format << ["cypher", "json", "xml", "yaml"]
    }
}
