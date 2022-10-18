package liquibase.ext.neo4j.change.refactoring


import liquibase.exception.LiquibaseException
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.database.Neo4jDatabase

class NodeMergerIT extends Neo4jContainerSpec {

    NodeMerger nodeMerger

    def setup() {
        nodeMerger = new NodeMerger(database as Neo4jDatabase)
    }

    def "generates statements to merge labels of the matching disconnected nodes"() {
        given:
        queryRunner.run("CREATE (:Label1:Label3), (:Label2:`Label Oops`), (:Label1:`Label Oops`)")
        def pattern = MatchPattern.of("(p)", "p")

        when:
        def statements = nodeMerger.merge(pattern, [])

        and:
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
            MATCH (n) UNWIND labels(n) AS label
            WITH label ORDER BY label ASC
            RETURN collect(label) AS labels
        """.stripIndent())
        row["labels"] == ["Label Oops", "Label1", "Label2", "Label3"]
    }

    def "does not generate any statement when less than 2 nodes are matching"(String graphInit, String fragment, String outputVariable) {
        given:
        if (graphInit != "") {
            queryRunner.run(graphInit)
        }

        expect:
        def statements = nodeMerger.merge(MatchPattern.of(fragment, outputVariable), [])
        statements.length == 0

        where:
        graphInit               | fragment  | outputVariable
        ""                      | "(n)"     | "n"
        "CREATE (:Foo)"         | "(n:Bar)" | "n"
        "CREATE (:Foo)"         | "(n)"     | "n"
        "CREATE (:Foo)"         | "(n:Foo)" | "n"
        "CREATE (:Foo), (:Bar)" | "(n:Foo)" | "n"
        "CREATE (:Foo), (:Bar)" | "(n:Bar)" | "n"
    }

    def "generates statements to merge properties of matching nodes based on each property's policy"(String propertyMatcher, PropertyMergeStrategy strategy, Object result) {
        given:
        queryRunner.run("CREATE (:Person), (:Person {name: 'Anastasia'}), (:Unmatched), (:Person {name: 'Zouheir'}), (:Person)")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        and:
        def statements = nodeMerger.merge(pattern, [PropertyMergePolicy.of(propertyMatcher, strategy)])
        statements.each queryRunner::run

        expect:
        def row = queryRunner.getSingleRow("MATCH (n:Person) RETURN n {.*}")
        row["n"]["name"] == result

        where:
        propertyMatcher | strategy                         | result
        "name"          | PropertyMergeStrategy.KEEP_ALL   | ["Anastasia", "Zouheir"]
        "name"          | PropertyMergeStrategy.KEEP_FIRST | "Anastasia"
        "name"          | PropertyMergeStrategy.KEEP_LAST  | "Zouheir"
        ".*"            | PropertyMergeStrategy.KEEP_ALL   | ["Anastasia", "Zouheir"]
        ".*"            | PropertyMergeStrategy.KEEP_FIRST | "Anastasia"
        ".*"            | PropertyMergeStrategy.KEEP_LAST  | "Zouheir"
    }

    def "fails to generate statements if mergeable properties do not have a policy"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia'}), (:Person {name: 'Zouheir'})")
        def unusedPolicy = PropertyMergePolicy.of("nom", PropertyMergeStrategy.KEEP_ALL)

        when:
        nodeMerger.merge(MatchPattern.of("(p:Person)", "p"), [unusedPolicy])

        then:
        def exc = thrown(LiquibaseException)
        exc.message == "could not find merge policy for node property name"
    }

    def "generates statements to merge incoming relationships"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia', age: 22})<-[:MAINTAINED_BY]-(:Project {name: 'Secret'}), (:Person {name: 'Zouheir'})<-[:FOUNDED_BY {year: 2012}]-(:Conference {name: 'Devoxx France'})")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of("name", PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(".*", PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)
WITH p {.*} AS person, [ (p)<-[incoming]-() | incoming {.*, type: type(incoming)} ] AS allIncoming
UNWIND allIncoming AS incoming
WITH person, incoming
ORDER BY incoming['type'] ASC
RETURN person AS p, collect(incoming) AS allIncoming
""")
        row["p"]["name"] == "Zouheir"
        row["p"]["age"] == 22
        row["allIncoming"] == [
                [type: "FOUNDED_BY", year: 2012],
                [type: "MAINTAINED_BY"],
        ]
    }

    def "generates statements to merge outgoing relationships"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia', age: 22})-[:MAINTAINS]->(:Project {name: 'Secret'}), (:Person {name: 'Zouheir'})-[:FOUNDED {year: 2012}]->(:Conference {name: 'Devoxx France'})")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of("name", PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(".*", PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)
WITH p {.*} AS person, [ (p)-[outgoing]->() | outgoing {.*, type: type(outgoing)} ] AS allOutgoing
UNWIND allOutgoing AS outgoing
WITH person, outgoing
ORDER BY outgoing['type'] ASC
RETURN person AS p, collect(outgoing) AS allOutgoing
""")
        row["p"]["name"] == "Zouheir"
        row["p"]["age"] == 22
        row["allOutgoing"] == [
                [type: "FOUNDED", year: 2012],
                [type: "MAINTAINS"],
        ]
    }

    def "escapes relationship types proper"() {
        given:
        queryRunner.run("CREATE (:Person {name: 'Anastasia', age: 42})<-[:MAINTAINED_BY]-(:Project {name: 'Secret'}), (:Conference {name: 'JavaLand'}) <-[:`HAT BESUCHT`]- (m:Person {name: 'Michael'})<-[:`FOUNDED BY` {year: 2015}]-(:JUG {name: 'EuregJUG'}), (m)-[:`DAS IST` {offensichtlich: true}]->(m)")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of("name", PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(".*", PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)
WITH p {.*} AS person, [ (p)-[outgoing]-() | outgoing {.*, type: type(outgoing)} ] AS rels
UNWIND rels AS rel
WITH person, rel
ORDER BY rel['type'] ASC
RETURN person AS p, collect(rel) AS rels
""")
        row["p"]["name"] == "Michael"
        row["p"]["age"] == 42
        row["rels"] == [
                [type: "DAS IST", 'offensichtlich': true],
                [type: "FOUNDED BY", year: 2015],
                [type: "HAT BESUCHT"],
                [type: "MAINTAINED_BY"],
        ]
    }

    def "generates statements that preserve existing self-relationships"() {
        given:
        queryRunner.run("CREATE (anastasia:Person {name: 'Anastasia', age: 22})-[:IS {obviously: true}]->(anastasia), (zouheir:Person {name: 'Zouheir'})-[:IS_SAME_AS {evidently: true}]->(zouheir)")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of("name", PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(".*", PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)-[r]->(p)
WITH r {.*, type: type(r)} AS rel
ORDER BY rel['type'] ASC
RETURN collect(rel) AS rels
""")
        row["rels"] == [
                [type: "IS", obviously: true],
                [type: "IS_SAME_AS", evidently: true],
        ]
    }

    def "generates statements that yields self-relationships"() {
        given:
        queryRunner.run("CREATE (anastasia:Person {name: 'Anastasia', age: 22})-[:FOLLOWS_1 {direction: 'a to z'}]->(zouheir:Person {name: 'Zouheir'}), " +
                "(zouheir)-[:FOLLOWS_2 {direction: 'z to a'}]->(anastasia)," +
                "(zouheir)-[:FOLLOWS_3 {direction: 'z to z'}]->(zouheir)," +
                "(zouheir)-[:FOLLOWS_4 {direction: 'z to m'}]->(:Person {name: 'Marouane'}) ")
        def pattern = MatchPattern.of("(p:Person) WITH p ORDER BY p.name ASC", "p")

        when:
        def statements = nodeMerger.merge(pattern, [
                PropertyMergePolicy.of("name", PropertyMergeStrategy.KEEP_LAST),
                PropertyMergePolicy.of(".*", PropertyMergeStrategy.KEEP_FIRST)
        ])
        statements.each queryRunner::run

        then:
        def row = queryRunner.getSingleRow("""
MATCH (p:Person)-[r]->(p)
WITH r {.*, type: type(r)} AS rel
ORDER BY rel['type'] ASC
RETURN collect(rel) AS rels
""")
        row["rels"] == [
                [type: "FOLLOWS_1", direction: "a to z"],
                [type: "FOLLOWS_2", direction: "z to a"],
                [type: "FOLLOWS_3", direction: "z to z"],
                [type: "FOLLOWS_4", direction: "z to m"],
        ]
    }
}
