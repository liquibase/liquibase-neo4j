package liquibase.ext.neo4j.change

import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.change.refactoring.ExtractedNodes
import liquibase.ext.neo4j.change.refactoring.ExtractedRelationships
import liquibase.ext.neo4j.change.refactoring.RelationshipDirection
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

import static liquibase.ext.neo4j.change.refactoring.RelationshipDirection.OUTGOING

class ExtractPropertyChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new ExtractPropertyChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects missing mandatory fields"() {
        given:
        def extractProperty = new ExtractPropertyChange()
        extractProperty.property = property
        extractProperty.fromNodes = fragment
        extractProperty.nodesNamed = outputVariable
        extractProperty.toNodes = extractedNodes

        expect:
        extractProperty.validate(new Neo4jDatabase()).getErrorMessages() == [error]

        where:
        property | fragment | outputVariable | extractedNodes                                      | error
        null     | "(n)"    | "n"            | toNodes("Foobar", "bar")                            | "missing property name"
        ""       | "(n)"    | "n"            | toNodes("Foobar", "bar")                            | "missing property name"
        "   "    | "(n)"    | "n"            | toNodes("Foobar", "bar")                            | "missing property name"
        "foo"    | null     | "n"            | toNodes("Foobar", "bar")                            | "missing Cypher fragment"
        "foo"    | ""       | "n"            | toNodes("Foobar", "bar")                            | "missing Cypher fragment"
        "foo"    | "  "     | "n"            | toNodes("Foobar", "bar")                            | "missing Cypher fragment"
        "foo"    | "(n)"    | null           | toNodes("Foobar", "bar")                            | "missing Cypher output variable"
        "foo"    | "(n)"    | ""             | toNodes("Foobar", "bar")                            | "missing Cypher output variable"
        "foo"    | "(n)"    | "   "          | toNodes("Foobar", "bar")                            | "missing Cypher output variable"
        "foo"    | "(n)"    | "n"            | null                                                | "missing node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes(null, "bar")                                | "missing label in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("", "bar")                                  | "missing label in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("   ", "bar")                               | "missing label in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", null)                             | "missing target property name in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "")                               | "missing target property name in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "   ")                            | "missing target property name in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "bar", withRels(null, OUTGOING))  | "missing relationship type in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "bar", withRels("", OUTGOING))    | "missing relationship type in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "bar", withRels("   ", OUTGOING)) | "missing relationship type in node extraction description"
        "foo"    | "(n)"    | "n"            | toNodes("Foobar", "bar", withRels("HAS", null))     | "missing relationship direction in node extraction description"
    }

    def "rejects reserved output variable name"() {
        given:
        def extractProperty = new ExtractPropertyChange()
        extractProperty.property = "foo"
        extractProperty.fromNodes = "(_____n_____)"
        extractProperty.nodesNamed = "_____n_____"
        extractProperty.toNodes = toNodes("Foobar", "bar")

        when:
        def validation = extractProperty.validate(new Neo4jDatabase())

        then:
        validation.getErrorMessages() == ["Cypher output variable \"_____n_____\" is reserved, please use another name"]
    }

    def "warns against merging relationships when creating nodes"() {
        given:
        def mergeNodes = false
        def mergeRels = true
        def extractProperty = new ExtractPropertyChange()
        extractProperty.property = "foo"
        extractProperty.fromNodes = "(f:ighters)"
        extractProperty.nodesNamed = "f"
        extractProperty.toNodes = toNodes("Foobar", "bar", withRels("HAS", OUTGOING, mergeRels), mergeNodes)

        when:
        def validation = extractProperty.validate(new Neo4jDatabase())

        then:
        validation.getWarningMessages() == ["creating nodes imply creating relationships - enable node merge or disable relation merge to suppress this warning"]
    }

    def "creates a confirmation message"() {
        given:
        def extractProperty = new ExtractPropertyChange()
        extractProperty.property = "source_prop"
        extractProperty.fromNodes = "(p:Person) WITH p ORDER BY p.birth_date ASC"

        when:
        def confirmationMessage = extractProperty.getConfirmationMessage()

        then:
        confirmationMessage == "property \"source_prop\" of nodes matching \"(p:Person) WITH p ORDER BY p.birth_date ASC\" has been extracted"
    }

    ExtractedNodes toNodes(String label, String property, ExtractedRelationships extractedRelationships = null, boolean merge = false) {
        def result = new ExtractedNodes()
        result.withLabel = label
        result.withProperty = property
        result.merge = merge
        result.linkedFromSource = extractedRelationships
        return result
    }

    ExtractedRelationships withRels(String type, RelationshipDirection direction, boolean merge = false) {
        def result = new ExtractedRelationships()
        result.withType = type
        result.withDirection = direction
        result.merge = merge
        return result
    }
}
