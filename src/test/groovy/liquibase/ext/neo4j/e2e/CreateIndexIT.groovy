package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.RollbackCountCommandStep
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class CreateIndexIT extends Neo4jContainerSpec {

    def cleanup() {
        queryRunner.dropIndex("movie_title_year", "Movie", "title")
        queryRunner.dropIndex("acted_in_role", "ACTED_IN", "role")
    }

    def "creates node and relationship indexes"() {
        when:
        runUpdate("/e2e/create-index/changeLog.${format}")

        then:
        def indices = queryRunner.listExistingIndices()
        indices.contains("movie_title_year:Movie,")
        indices.contains("acted_in_role:ACTED_IN,")

        where:
        format << ["json", "yaml"]
    }

    def "creates composite index with the declared properties"() {
        when:
        runUpdate("/e2e/create-index/changeLog.yaml")

        then:
        def row = queryRunner.getSingleRow("""
            SHOW INDEXES YIELD name, properties
            WHERE name = 'movie_title_year'
            RETURN properties
        """)
        Arrays.asList((String[]) row["properties"]) == ["title", "year"]
    }

    def "does not fail when the index already exists and ifNotExists is set"() {
        given:
        queryRunner.createIndex("movie_title_year", "Movie", "title")

        when:
        runUpdate("/e2e/create-index/changeLog.yaml")

        then:
        noExceptionThrown()
        queryRunner.listExistingIndices().contains("movie_title_year:Movie,")
    }

    def "rolls back by dropping the named indexes"() {
        given:
        runUpdate("/e2e/create-index/changeLog.yaml")

        when:
        runRollback("/e2e/create-index/changeLog.yaml", 2)

        then:
        def indices = queryRunner.listExistingIndices()
        !indices.contains("movie_title_year:Movie,")
        !indices.contains("acted_in_role:ACTED_IN,")
    }

    private void runUpdate(String changeLogFile) {
        new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogFile)
                .setOutput(System.out)
                .execute()
    }

    private void runRollback(String changeLogFile, int count) {
        new CommandScope(RollbackCountCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogFile)
                .addArgumentValue(RollbackCountCommandStep.COUNT_ARG, count)
                .setOutput(System.out)
                .execute()
    }
}
