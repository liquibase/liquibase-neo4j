package liquibase.ext.neo4j.parser

import liquibase.Scope
import liquibase.change.core.RawSQLChange
import liquibase.changelog.ChangeLogParameters
import liquibase.exception.ChangeLogParseException
import liquibase.resource.ResourceAccessor
import liquibase.sdk.resource.MockResourceAccessor
import spock.lang.Specification

class AsciidocChangeLogParserTest extends Specification {

    String scopeId

    void cleanup() {
        if (scopeId != null) {
            Scope.exit(scopeId)
        }
    }

    def "parses Liquibase code listings"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        changeLog.physicalFilePath == "changelog.adoc"
        def changeSets = changeLog.getChangeSets()
        changeSets.size() == 1
        def changeSet = changeSets.first()
        changeSet.id == "baz-creation"
        changeSet.author == "fbiville"
        !changeSet.alwaysRun
        !changeSet.runOnChange
        changeSet.runInTransaction
        changeSet.contextFilter.originalString == ""
        changeSet.dbmsSet == null
        changeSet.changes.size() == 1
        ((RawSQLChange) changeSet.changes.first()).sql == "CREATE (:Baz {name: \"no-name\"})"
    }

    def "parses Liquibase code listings with change log parameters"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()
        def parameters = new ChangeLogParameters()
        parameters.set("database.supportsSequences", false);


        when:
        def changeLog = parser.parse("changelog.adoc", parameters, accessor)

        then:
        changeLog.changeLogParameters == parameters
    }

    def "parses nested Liquibase code listings"() {
        given:
        def accessor = new MockResourceAccessor(["first.adoc"    : """
[source,cypher,target=liquibase,id=first,author=fbiville]
----
CREATE (:First)
----
""",
                                                 "changelog.adoc": """
include::first.adoc[]

[source,cypher,target=liquibase,id=second,author=fbiville]
----
CREATE (:Second)
----

include::third.adoc[]
""",
                                                 "third.adoc"    : """
[source,cypher,target=liquibase,id=third,author=fbiville]
----
CREATE (:Third)
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        changeLog.physicalFilePath == "changelog.adoc"
        def changeSets = changeLog.getChangeSets()
        changeSets.size() == 3
        changeSets[0].id == "first"
        changeSets[0].author == "fbiville"
        ((RawSQLChange) changeSets[0].changes.first()).sql == "CREATE (:First)"
        changeSets[1].id == "second"
        changeSets[1].author == "fbiville"
        ((RawSQLChange) changeSets[1].changes.first()).sql == "CREATE (:Second)"
        changeSets[2].id == "third"
        changeSets[2].author == "fbiville"
        ((RawSQLChange) changeSets[2].changes.first()).sql == "CREATE (:Third)"
    }

    def "parses Liquibase code listings that always run"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runAlways=true]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        def changeset = changeLog.getChangeSets().first()
        changeset.id == "baz-creation"
        changeset.author == "fbiville"
        changeset.alwaysRun
    }

    def "parses Liquibase code listings that run on change"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runOnChange=true]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        def changeset = changeLog.getChangeSets().first()
        changeset.id == "baz-creation"
        changeset.author == "fbiville"
        changeset.runOnChange
    }

    def "parses Liquibase code listings that target one DBMS"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,dbms=neo4j]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        def changeset = changeLog.getChangeSets().first()
        changeset.id == "baz-creation"
        changeset.author == "fbiville"
        changeset.dbmsSet == ["neo4j"].toSet()
    }

    def "parses Liquibase code listings that target two DBMS"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,dbms="neo4j,postgres"]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        def changeset = changeLog.getChangeSets().first()
        changeset.id == "baz-creation"
        changeset.author == "fbiville"
        changeset.dbmsSet == ["neo4j", "postgres"].toSet()
    }

    def "parses Liquibase code listings that does not run in transaction"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runInTransaction=false]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        scopeId = Scope.enter(withResourceAccessor(accessor))
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        def changeset = changeLog.getChangeSets().first()
        changeset.id == "baz-creation"
        changeset.author == "fbiville"
        !changeset.runInTransaction
    }

    def "ignores non Liquibase code listings"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher]
----
RETURN 42
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        def changeLog = parser.parse("changelog.adoc", null, accessor)

        then:
        changeLog.getChangeSets().isEmpty()
    }

    def "rejects Liquibase code listings without ID"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,author=fbiville]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("changelog.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException.class)
        exception.message == "Liquibase code listing ID attribute must be set"
    }

    def "rejects Liquibase code listings without author"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("changelog.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException.class)
        exception.message == "Liquibase code listing author attribute must be set"
    }

    def "rejects Liquibase code listings with erroneous runAlways values"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runAlways=42]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("changelog.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException)
        exception.message == """Liquibase code listing runAlways attribute must be set to either "true" or "false", found: 42"""
    }

    def "rejects Liquibase code listings with erroneous runOnChange values"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runOnChange=42]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("changelog.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException)
        exception.message == """Liquibase code listing runOnChange attribute must be set to either "true" or "false", found: 42"""
    }

    def "rejects Liquibase code listings with erroneous runInTransaction values"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
[source,cypher,target=liquibase,id=baz-creation,author=fbiville,runInTransaction=42]
----
CREATE (:Baz {name: "no-name"})
----
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("changelog.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException)
        exception.message == """Liquibase code listing runInTransaction attribute must be set to either "true" or "false", found: 42"""
    }

    def "fails if Asciidoc change log is not found"() {
        given:
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("unknown.adoc", null, new MockResourceAccessor())

        then:
        def exception = thrown(ChangeLogParseException)
        exception.cause instanceof IOException
        exception.cause.message == "could not find change log resource unknown.adoc"
    }

    def "fails if nested Asciidoc change log is not found"() {
        given:
        def accessor = new MockResourceAccessor(["changelog.adoc": """
include::unknown.adoc[]
"""])
        def parser = new AsciidocChangeLogParser()

        when:
        parser.parse("unknown.adoc", null, accessor)

        then:
        def exception = thrown(ChangeLogParseException)
        exception.cause instanceof IOException
        exception.cause.message == "could not find change log resource unknown.adoc"
    }


    private Map<String, Object> withResourceAccessor(ResourceAccessor accessor) {
        Map<String, Object> values = [:]
        values[Scope.Attr.resourceAccessor.name()] = accessor
        values
    }

}
