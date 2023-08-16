# Download

## Prerequisites

First install [Liquibase](https://www.liquibase.org/download).

## Installation

=== "CLI users"
    1. Locate the Liquibase installation folder (subsequently called `LIQUIBASE_HOME`)
    1. Download the extension JAR (from [GitHub]({{ github_repo }}/releases/download/{{ artifact_id }}-{{ version
    }}/{{ artifact_id }}-{{ version }}-full.jar))
    1. Place the JAR in the `lib` folder of `LIQUIBASE_HOME`
    1. Run `liquibase --version`, the Neo4j extension should be listed

=== "Maven users"

    === "Regular users"

        Make sure to add the `{{ artifact_id }}` dependency as follows, alongside your other dependencies.

        ```xml
        <dependency>
            <groupId>{{ group_id }}</groupId>
            <artifactId>{{ artifact_id }}</artifactId>
            <version>{{ version }}</version>
        </dependency>
        ```
        !!! info
            The Neo4j extension requires the `org:liquibase:liquibase-core` dependency to be included.
            If you are a Spring Framework or Spring Boot user for instance, `liquibase-core` is likely a transitive dependency and
            may not appear directly in your project file.
            Make sure `liquibase-core` is resolved in your dependency tree (run `mvn dependency:tree` to display the latter).

    === "Liquibase plugin users"

        Make sure to add the `{{ artifact_id }}` dependency to the plugin definition.

        ```xml
        <plugin>
            <groupId>org.liquibase</groupId>
            <artifactId>liquibase-maven-plugin</artifactId>
            <version>{{ liquibase_version }}</version>
            <dependencies>
                <dependency>
                    <groupId>{{ group_id }}</groupId>
                    <artifactId>{{ artifact_id }}</artifactId>
                    <version>{{ version }}</version>
                </dependency>
            </dependencies>
        </plugin>
        ```

=== "Gradle users"

    === "Regular users"

        Make sure to add the `{{ artifact_id }}` dependency as follows, alongside your other dependencies.

        ```groovy
        runtimeOnly '{{ group_id }}:{{ artifact_id }}:{{ version }}'
        ```

        !!! info
            The Neo4j extension requires the `org:liquibase:liquibase-core` dependency to be included.
            If you are a Spring Framework or Spring Boot user for instance, `liquibase-core` is likely a transitive dependency and
            may not appear directly in your project file.
            Make sure `liquibase-core` is resolved in your dependency tree (run `gradle -q dependencies` to display the latter).

    === "Liquibase plugin users"

        Make sure to add the `{{ artifact_id }}` dependency as follows.

        ```groovy
        liquibaseRuntime '{{ group_id }}:{{ artifact_id }}:{{ version }}'
        ```

## :fontawesome-solid-circle-radiation: Unsupported versions

The following versions of Liquibase core are not compatible with the Neo4j extension:

| Version | Workaround                                                                                                                                                        |
|---------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 4.23.0  | [recommended] Upgrade both core and the extension to 4.23.1 (or later). <br/>Alternatively, use Liquibase core 4.21.1 and the Neo4j extension at version 4.21.1.2 |


{! include-markdown 'includes/_abbreviations.md' !}
