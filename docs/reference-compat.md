# Compatibility Guarantees

`liquibase-neo4j` is a **plugin** for Liquibase.

It provides Neo4j-specific features through various Liquibase API.

Most of these API are invisible to the end user.

As such, it does not provide the same set of compatibility guarantees as a regular Java library.

## Breaking Changes

The following lists what constitutes a breaking change and warrants a bump in the major version of the
plugin.

Anything that is **not** listed below is therefore not considered a breaking change and may be released at any time.

Since `liquibase-neo4j` releases are coordinated with Liquibase core, such breaking changes are only allowed when
Liquibase releases a new major version.

Learn about the plugin versioning scheme [here](/#liquibase).

**Breaking changes**

- Bumped minimum JDK major version (e.g. JDK 17 required instead of JDK 8)
- Rename of a serialized change name or any of its child elements (e.g. `<cypher>` is renamed to `<query>`)
- Rename of a serialized attribute (e.g. `<extractProperty property="foo">` is renamed
  to `<extractProperty key="foo">`)
- Rename of a serialized enumeration Java type (e.g. `liquibase.ext.neo4j.change.refactoring.RelationshipDirection` is
  moved to `graph.RelationshipDirection`)<sup>1</sup>
- Change of an attribute default value<sup>2</sup>

!!! notes
    <sup>1</sup> This oddity is due to the way YAML change sets are serialized. The enumeration Java type is added to
    the raw string as a deserialization hint to the YAML parser, see e.g.:
    ```yaml
    extractedRelationships:
      withDirection: !!liquibase.ext.neo4j.change.refactoring.RelationshipDirection 'OUTGOING'
    ```
    <sup>2</sup> This is technically not a breaking change, but the change of default value is likely to cause a change in
    behavior for the end users. Such changes must be planned carefully and announced well prior to the introduction of said
    change.

{!includes/_abbreviations.md!}
