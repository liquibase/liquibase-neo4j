package liquibase.ext.neo4j.structure

import spock.lang.Specification

class GraphTypeTruncatorTest extends Specification {

    def "appends closing symbols to truncated spec"() {
        given:
        def spec = """{
  (:`Company` => {`address` :: STRING, `name` :: STRING}),
  (:`Person`)-[:`WORKS_FOR` => {`role` :: STRING}]->(:`Company` =>),
  CONSTRAINT `constraint_1324d6fc` FOR (`n`:`Company` =>) REQUIRE (`n`.`address`) IS UNIQUE
}"""

        when:
        def result = GraphTypeTruncator.truncate(spec)

        then:
        result == "{  (:`Company` => {`address` :: STRING, `name` ::  [...] } ) }"
    }

    def "keeps short spec as is"() {
        given:
        def spec = """{}"""

        when:
        def result = GraphTypeTruncator.truncate(spec)

        then:
        result == "{}"
    }
}
