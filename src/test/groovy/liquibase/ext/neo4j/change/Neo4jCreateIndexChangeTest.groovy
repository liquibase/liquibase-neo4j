package liquibase.ext.neo4j.change

import liquibase.change.AddColumnConfig
import liquibase.database.core.MySQLDatabase
import liquibase.exception.RollbackImpossibleException
import liquibase.ext.neo4j.database.Neo4jDatabase
import liquibase.parser.core.yaml.YamlChangeLogParser
import liquibase.changelog.ChangeLogParameters
import liquibase.resource.DirectoryResourceAccessor
import spock.lang.Specification

class Neo4jCreateIndexChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new Neo4jCreateIndexChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def change = new Neo4jCreateIndexChange()
        change.labelName = label
        change.relationshipType = type
        columns.each { change.addColumn(new AddColumnConfig().setName(it)) }

        expect:
        change.validate(new Neo4jDatabase()).errorMessages == errors

        where:
        label   | type    | columns   | errors
        null    | null    | ["a"]     | ["exactly one of labelName or relationshipType must be specified and not blank"]
        "Movie" | "SEEN"  | ["a"]     | ["exactly one of labelName or relationshipType must be specified and not blank"]
        "Movie" | null    | []        | ["at least one column (indexed property) must be specified"]
        "Movie" | null    | [" "]     | ["column names must be specified and not blank"]
        "Movie" | null    | ["a"]     | []
        null    | "SEEN"  | ["a", "b"] | []
    }

    def "generates Cypher"() {
        given:
        def change = new Neo4jCreateIndexChange()
        change.indexName = name
        change.labelName = label
        change.relationshipType = type
        change.ifNotExists = ifNotExists
        columns.each { change.addColumn(new AddColumnConfig().setName(it)) }

        expect:
        change.generateStatements(new Neo4jDatabase())[0].sql == cypher

        where:
        name   | label   | type    | ifNotExists | columns    | cypher
        "idx"  | "Movie" | null    | null        | ["title"]  | "CREATE INDEX `idx` FOR (n:`Movie`) ON (n.`title`)"
        "idx"  | "Movie" | null    | true        | ["a", "b"] | "CREATE INDEX `idx` IF NOT EXISTS FOR (n:`Movie`) ON (n.`a`, n.`b`)"
        null   | null    | "SEEN"  | false       | ["role"]   | "CREATE INDEX FOR ()-[n:`SEEN`]-() ON (n.`role`)"
        "a`b"  | "Movie" | null    | null        | ["title"]  | "CREATE INDEX `a``b` FOR (n:`Movie`) ON (n.`title`)"
    }

    def "rolls back by dropping the index"() {
        given:
        def change = new Neo4jCreateIndexChange()
        change.indexName = "idx"

        expect:
        change.supportsRollback(new Neo4jDatabase())
        change.generateRollbackStatements(new Neo4jDatabase())[0].sql == "DROP INDEX `idx` IF EXISTS"
    }

    def "cannot roll back unnamed index"() {
        given:
        def change = new Neo4jCreateIndexChange()

        when:
        change.generateRollbackStatements(new Neo4jDatabase())

        then:
        !change.supportsRollback(new Neo4jDatabase())
        thrown(RollbackImpossibleException)
    }

    def "loads from YAML changelog"() {
        given:
        def accessor = new DirectoryResourceAccessor(new File("src/test/resources"))
        def changeLog = new YamlChangeLogParser().parse("/e2e/create-index/changeLog.yaml", new ChangeLogParameters(), accessor)

        when:
        def node = changeLog.changeSets[0].changes[0] as Neo4jCreateIndexChange
        def rel = changeLog.changeSets[1].changes[0] as Neo4jCreateIndexChange

        then:
        node.indexName == "movie_title_year"
        node.labelName == "Movie"
        node.ifNotExists == true
        node.columns*.name == ["title", "year"]
        rel.relationshipType == "ACTED_IN"
        rel.columns*.name == ["role"]
    }
}
