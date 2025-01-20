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
        version     | result
        "1.0.0"     | new KernelVersion(1, 0, 0)
        "1.0"       | new KernelVersion(1, 0)
        "5.26-aura" | new KernelVersion(5, 26)
        "5"         | new KernelVersion(5)
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
