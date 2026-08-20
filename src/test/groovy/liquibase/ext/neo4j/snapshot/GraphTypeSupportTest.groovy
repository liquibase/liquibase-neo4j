package liquibase.ext.neo4j.snapshot

import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
import liquibase.statement.core.RawParameterizedSqlStatement
import liquibase.structure.core.Catalog
import spock.lang.Specification

import static liquibase.ext.neo4j.database.KernelVersion.V2025_10_0
import static liquibase.ext.neo4j.database.KernelVersion.V2026_02_0
import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0
import static liquibase.ext.neo4j.database.KernelVersion.V5_26_0

class GraphTypeSupportTest extends Specification {

    def "loads graph type metadata when the current graph type is set"() {
        given:
        def catalog = new Catalog("neo4j")
        def database = Mock(Neo4jDatabase)
        database.isEnterprise() >> true
        database.getKernelVersion() >> V2026_02_0

        when:
        def graphType = GraphTypeSupport.load(catalog, database)

        then:
        1 * database.run(catalog, { RawParameterizedSqlStatement statement ->
            statement.sql == 'CYPHER 25 SHOW CURRENT GRAPH TYPE YIELD specification RETURN specification'
        }) >> [[specification: "(:Person => {name :: STRING})", set: true]]
        graphType.present
        graphType.get().getAttribute("specification", String) == "(:Person => {name :: STRING})"
    }

    def "does not load graph type metadata when graph type is not supported"(boolean isEnterprise, KernelVersion version) {
        given:
        def database = Mock(Neo4jDatabase)
        database.isEnterprise() >> isEnterprise
        database.getKernelVersion() >> version

        expect:
        !GraphTypeSupport.load(new Catalog("neo4j"), database).present

        where:
        isEnterprise | version
        false        | V4_4_0
        true         | V4_4_0
        false        | V5_26_0
        true         | V5_26_0
        false        | V2025_10_0
        true         | V2025_10_0
        false        | V2026_02_0
    }
}
