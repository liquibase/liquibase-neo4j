package liquibase.ext.neo4j.change.refactoring

import liquibase.ext.neo4j.Neo4jContainerSpec

class PropertyExtractorIT extends Neo4jContainerSpec {

    PropertyExtractor nodePropertyExtractor

    def setup() {
        queryRunner.run(
                """
            CREATE (:Person {name:'Florent', `country name`: 'TR'})
            CREATE (:Person {name:'Marouane', `country name`: 'MA'})
            CREATE (p:Person {name:'Chaibia', `country name`: 'MA'})
            CREATE (:Person {name: 'Nathan'})
            CREATE (:Person {name:'Dilek', `country name`: 'FR'})

            CREATE (c:`My Country` {country: 'MA'})
            CREATE (:`My Country` {country: 'JP'})
            MERGE (p)-[:`BELONGS TO`]-(c)
            """)

        nodePropertyExtractor = new PropertyExtractor()
    }

    def "generates statements to merge extracted nodes without creating relationships"() {
        given:
        def pattern = PropertyExtraction.of(
                MatchPattern.of("(p:Person) WHERE p.`country name` IS NOT NULL WITH p ORDER BY p.`country name` ASC", "p"),
                NodeExtraction.merging("My Country", "country name", "country"))

        when:
        def statements = nodePropertyExtractor.extract(pattern)

        and:
        statements.each queryRunner::run

        then:
        def persons = getPeople()

        def countries = getCountries()

        persons == [[p: ["name": "Chaibia"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                    [p: ["name": "Dilek"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Florent"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Marouane"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Nathan"], c: null, "relationship": null, "startNode": null]
        ]

        countries == [
                [c: ["country": "FR"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "JP"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "MA"], p: ["name": "Chaibia"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                [c: ["country": "TR"], p: null, "relationship": null, "startNode": null]

        ]
    }

    def "generates statements to create labels of the matching disconnected nodes"() {
        given:
        def pattern = PropertyExtraction.of(
                MatchPattern.of("(p:Person) WHERE p.`country name` IS NOT NULL WITH p ORDER BY p.country", "p"),
                NodeExtraction.creating("My Country", "country name", "country"))

        when:
        def statements = nodePropertyExtractor.extract(pattern)

        and:
        statements.each queryRunner::run

        then:
        def persons = getPeople()

        def countries = getCountries()

        persons == [[p: ["name": "Chaibia"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                    [p: ["name": "Dilek"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Florent"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Marouane"], c: null, "relationship": null, "startNode": null],
                    [p: ["name": "Nathan"], c: null, "relationship": null, "startNode": null]
        ]

        countries == [
                [c: ["country": "FR"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "JP"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "MA"], p: ["name": "Chaibia"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                [c: ["country": "MA"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "MA"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "TR"], p: null, "relationship": null, "startNode": null]
        ]
    }


    def "generates statements to merge relationships of the matching connected nodes"() {
        given:
        def pattern = PropertyExtraction.of(
                MatchPattern.of("(p:Person) WHERE p.`country name` IS NOT NULL WITH p ORDER BY p.country", "p"),
                NodeExtraction.merging("My Country", "country name", "country"),
                RelationshipExtraction.merging("BELONGS TO", RelationshipDirection.OUTGOING))

        when:
        def statements = nodePropertyExtractor.extract(pattern)

        and:
        statements.each queryRunner::run

        then:
        def persons = getPeople()

        def countries = getCountries()

        persons == [[p: ["name": "Chaibia"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                    [p: ["name": "Dilek"], c: ["country": "FR"], "relationship": "BELONGS TO", "startNode": "Dilek"],
                    [p: ["name": "Florent"], c: ["country": "TR"], "relationship": "BELONGS TO", "startNode": "Florent"],
                    [p: ["name": "Marouane"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Marouane"],
                    [p: ["name": "Nathan"], c: null, "relationship": null, "startNode": null]
        ]

        countries == [
                [c: ["country": "FR"], p: ["name": "Dilek"], "relationship": "BELONGS TO", "startNode": "Dilek"],
                [c: ["country": "JP"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "MA"], p: ["name": "Chaibia"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                [c: ["country": "MA"], p: ["name": "Marouane"], "relationship": "BELONGS TO", "startNode": "Marouane"],
                [c: ["country": "TR"], p: ["name": "Florent"], "relationship": "BELONGS TO", "startNode": "Florent"],
        ]
    }

    def "generates statements to create relationships of the matching connected nodes"() {
        given:
        def pattern = PropertyExtraction.of(
                MatchPattern.of("(p:Person) WHERE p.`country name` IS NOT NULL WITH p ORDER BY p.country", "p"),
                NodeExtraction.merging("My Country", "country name", "country"),
                RelationshipExtraction.creating("BELONGS TO", RelationshipDirection.OUTGOING))

        when:
        def statements = nodePropertyExtractor.extract(pattern)

        and:
        statements.each queryRunner::run

        then:
        def persons = getPeople()

        def countries = getCountries()

        def countriesCount = queryRunner.getSingleRow("MATCH (c:`My Country`) RETURN COUNT(c) as countries")

        persons == [[p: ["name": "Chaibia"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                    [p: ["name": "Chaibia"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                    [p: ["name": "Dilek"], c: ["country": "FR"], "relationship": "BELONGS TO", "startNode": "Dilek"],
                    [p: ["name": "Florent"], c: ["country": "TR"], "relationship": "BELONGS TO", "startNode": "Florent"],
                    [p: ["name": "Marouane"], c: ["country": "MA"], "relationship": "BELONGS TO", "startNode": "Marouane"],
                    [p: ["name": "Nathan"], c: null, "relationship": null, "startNode": null]
        ]

        countries == [
                [c: ["country": "FR"], p: ["name": "Dilek"], "relationship": "BELONGS TO", "startNode": "Dilek"],
                [c: ["country": "JP"], p: null, "relationship": null, "startNode": null],
                [c: ["country": "MA"], p: ["name": "Chaibia"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                [c: ["country": "MA"], p: ["name": "Chaibia"], "relationship": "BELONGS TO", "startNode": "Chaibia"],
                [c: ["country": "MA"], p: ["name": "Marouane"], "relationship": "BELONGS TO", "startNode": "Marouane"],
                [c: ["country": "TR"], p: ["name": "Florent"], "relationship": "BELONGS TO", "startNode": "Florent"],
        ]

        countriesCount == ["countries": 4]
    }

    private List<Map<String, Object>> getPeople() {
        queryRunner.getRows("""
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[r:`BELONGS TO`]->(c:`My Country`)
            WITH p,c,r
            ORDER BY p.name, c.country
            RETURN p{.*}, c{.*}, type(r) AS relationship, startNode(r).name as startNode
        """.stripIndent())
    }

    private List<Map<String, Object>> getCountries() {
        queryRunner.getRows("""
            MATCH (c:`My Country`)
            OPTIONAL MATCH (p)-[r:`BELONGS TO`]->(c)
            WITH p,c,r
            ORDER BY c.country, p.name
            RETURN p{.*}, c{.*}, type(r) AS relationship, startNode(r).name as startNode
        """.stripIndent())
    }

}
