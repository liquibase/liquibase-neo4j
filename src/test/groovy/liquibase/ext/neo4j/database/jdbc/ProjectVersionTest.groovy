package liquibase.ext.neo4j.database.jdbc

import spock.lang.Specification

class ProjectVersionTest extends Specification {

    def "parses project version"() {
        when:
        def version = ProjectVersion.parse()

        then:
        version.major >= 4
        version.minor >= 0
    }
}
