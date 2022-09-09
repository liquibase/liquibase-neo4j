# Neo4j Plugin for Liquibase

Database refactoring automation for [Neo4j](https://neo4j.com), the leading graph database.

## About

The Neo4j Plugin for Liquibase (abbreviated `liquibase-neo4j` in the rest of the documentation) is a Neo4j extension
for [Liquibase](https://www.liquibase.org/).
The extension allows to manage changes specifically for Neo4j databases.

## Versions

### Neo4j

The minimum required version of Neo4j is 3.5.
The extension supports both Community Edition and Enterprise Edition of Neo4j.
Please make sure to verify the end of support date of Neo4j
versions [here](https://neo4j.com/developer/kb/neo4j-supported-versions/).

### Liquibase

The versioning scheme of the Neo4j extension follows the versioning scheme
of [Liquibase core](https://github.com/liquibase/liquibase).
The scheme is defined as follows: `major.minor.patch(.extra)?`:

- `major`: major version of Liquibase core that the extension was released with
- `minor`: minor version of Liquibase core
- `patch`: patch version of Liquibase core
- `extra`: optional subsequent release of the extension, supporting Liquibase core at version `Major.minor.patch`

The extension is guaranteed to work with the corresponding Liquibase core version.
It may also work with previous and subsequent Liquibase core releases but that is not guaranteed.

For instance, `liquibase-neo4j` 4.15.0.3 is guaranteed to work with Liquibase core 4.15.0.
It may also work with older and newer Liquibase core versions, but that is not always the case.

!!! note
    [Release notes](https://github.com/liquibase/liquibase-neo4j/releases) detail when a specific version of
    Liquibase core is required.

{!includes/_abbreviations.md!}