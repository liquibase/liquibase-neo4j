package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V5_21_0
import static org.junit.jupiter.api.Assumptions.assumeTrue

class NormalizeBooleanIT extends Neo4jContainerSpec {

    def "normalizes string boolean properties on nodes and relationships"() {
        given:
        runUpdate("/e2e/normalize-boolean/changeLog.${format}")

        expect:
        assertNormalizedBooleans(false)

        where:
        format << ["json", "xml", "yaml"]
    }

    def "normalizes string boolean properties in batched migrations"() {
        given:
        if (concurrent) {
            assumeTrue(neo4jVersion() >= V5_21_0)
        }
        def suffix = concurrent ? "-batched-concurrent" : "-batched"
        runUpdate("/e2e/normalize-boolean/changeLog${suffix}.${format}")

        expect:
        assertNormalizedBooleans(false)

        where:
        [format, concurrent] << [["json", "xml", "yaml"], [false, true]].combinations()
    }

    def "deletes unmatched values when deleteUnmatched is true"() {
        given:
        runUpdate("/e2e/normalize-boolean/changeLog-delete-unmatched.${format}")

        expect:
        assertNormalizedBooleans(true)

        where:
        format << ["json", "xml", "yaml"]
    }

    private void runUpdate(String changelogPath) {
        new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, changelogPath)
                .setOutput(System.out)
                .execute()
    }

    private void assertNormalizedBooleans(boolean deleteUnmatched) {
        def movieCounts = queryRunner.getSingleRow("""
            MATCH (m:Movie)
            RETURN
                count(CASE WHEN m.watched = true THEN 1 END) AS trueCount,
                count(CASE WHEN m.watched = false THEN 1 END) AS falseCount,
                count(CASE WHEN m.watched IS NULL THEN 1 END) AS missingCount
        """)

        assert movieCounts["trueCount"] == 2
        assert movieCounts["falseCount"] == 1
        assert movieCounts["missingCount"] == (deleteUnmatched ? 1 : 0)

        if (!deleteUnmatched) {
            def maybe = queryRunner.getSingleRow("""
                MATCH (m:Movie {watched: 'maybe'})
                RETURN count(m) AS count
            """)
            assert maybe["count"] == 1
        }

        def relationship = queryRunner.getSingleRow("""
            MATCH ()-[r:SEEN]->()
            RETURN r.watched AS watched
        """)

        assert relationship["watched"] == true
    }
}
