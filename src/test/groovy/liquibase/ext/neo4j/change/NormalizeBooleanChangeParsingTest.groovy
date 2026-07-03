package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeLogParameters
import liquibase.parser.core.json.JsonChangeLogParser
import liquibase.parser.core.xml.XMLChangeLogSAXParser
import liquibase.parser.core.yaml.YamlChangeLogParser
import liquibase.resource.DirectoryResourceAccessor
import spock.lang.Specification

class NormalizeBooleanChangeParsingTest extends Specification {

    def accessor = new DirectoryResourceAccessor(new File("src/test/resources"))

    def "loads trueValues and falseValues from #format changelog"() {
        given:
        def changeLog = parser.parse("/e2e/normalize-boolean/changeLog.${format}", new ChangeLogParameters(), accessor)
        def change = changeLog.changeSets[1].changes[0] as NormalizeBooleanChange

        expect:
        change.trueValues == "YES,y"
        change.falseValues == "no,n"

        where:
        format | parser
        "json" | new JsonChangeLogParser()
        "yaml" | new YamlChangeLogParser()
        "xml"  | new XMLChangeLogSAXParser()
    }
}
