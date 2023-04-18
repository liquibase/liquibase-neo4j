package liquibase.ext.neo4j.change.refactoring

import spock.lang.Specification

class PropertyMergePolicyTest extends Specification {

    def "rejects null property matcher"() {
        given:
        def policy = new PropertyMergePolicy()

        when:
        policy.setNameMatcher(null)

        then:
        def exception = thrown(IllegalArgumentException.class)
        exception.message == "property matcher of property merge policy cannot be null"
    }
}
