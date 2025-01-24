package liquibase.ext.neo4j.database

import spock.lang.Specification

import static liquibase.ext.neo4j.database.KernelVersionTest.Sign.NEGATIVE
import static liquibase.ext.neo4j.database.KernelVersionTest.Sign.POSITIVE
import static liquibase.ext.neo4j.database.KernelVersionTest.Sign.ZERO

class KernelVersionTest extends Specification {

    def "parses versions"() {
        expect:
        KernelVersion.parse(version) == result

        where:
        version          | result
        "1.0.0"          | new KernelVersion(1, 0, 0)
        "1.0.12"         | new KernelVersion(1, 0, 12)
        "1.0"            | new KernelVersion(1, 0)
        "5.26-aura"      | new KernelVersion(5, 26)
        "5"              | new KernelVersion(5)
        "2025.01-aura"   | new KernelVersion(2025, 1)
        "2025.1.2"       | new KernelVersion(2025, 1, 2)
        "2025.1.2-93482" | new KernelVersion(2025, 1, 2)
    }

    def "rejects invalid versions"() {
        when:
        KernelVersion.parse(version)

        then:
        thrown(exceptionType)

        where:
        version      | exceptionType
        ""           | IllegalArgumentException.class
        "foobar"     | NumberFormatException.class
        "."          | NumberFormatException.class
        ".."         | NumberFormatException.class
        "5."         | IllegalArgumentException.class
        "2025.1."    | IllegalArgumentException.class
        ".5.25"      | NumberFormatException.class
        "2025.2.1.2" | IllegalArgumentException.class
    }

    def "compares versions"() {
        expect:
        Sign.from(version1 <=> version2) == sign

        where:
        version1         | version2         | sign
        version(1, 0, 0) | version(1, 0, 0) | ZERO
        version(1, 0, 1) | version(1, 0, 0) | POSITIVE
        version(1)       | version(1, 0, 0) | POSITIVE
        version(1, 0)    | version(1, 0, 0) | POSITIVE
        version(1, 1, 0) | version(1, 0, 0) | POSITIVE
        version(1, 1)    | version(1, 0, 0) | POSITIVE
        version(2, 0, 0) | version(1, 0, 0) | POSITIVE
        version(2, 0)    | version(1, 0, 0) | POSITIVE
        version(1, 0, 0) | version(1, 0, 1) | NEGATIVE
        version(1, 0, 0) | version(1)       | NEGATIVE
        version(1, 0, 0) | version(1, 0)    | NEGATIVE
        version(1, 0, 0) | version(1, 1, 0) | NEGATIVE
        version(1, 0, 0) | version(1, 1)    | NEGATIVE
        version(1, 0, 0) | version(2, 0, 0) | NEGATIVE
        version(1, 0, 0) | version(2, 0)    | NEGATIVE
    }

    private static KernelVersion version(int major) {
        new KernelVersion(major)
    }

    private static KernelVersion version(int major, int minor) {
        new KernelVersion(major, minor)
    }

    private static KernelVersion version(int major, int minor, int patch) {
        new KernelVersion(major, minor, patch)
    }

    enum Sign {
        NEGATIVE, ZERO, POSITIVE

        static Sign from(int result) {
            if (result > 0) return POSITIVE
            if (result < 0) return NEGATIVE
            return ZERO
        }
    }
}
