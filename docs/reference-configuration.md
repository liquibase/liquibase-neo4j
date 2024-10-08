# Neo4j Configuration

## Supported URIs

`liquibase-neo4j` accepts only URIs in the JDBC format.

Only connections through the Bolt protocol variants are supported.
Connections through HTTP or embedded are not supported by the extension.

- ✅ `jdbc:neo4j:bolt://host:port` is supported
- ✅ `jdbc:neo4j:bolt+s://host:port` is supported
- ✅ `jdbc:neo4j:bolt+ssc://host:port` is supported
- ✅ `jdbc:neo4j:neo4j://host:port` is supported
- ✅ `jdbc:neo4j:neo4j+s://host:port` is supported
- ✅ `jdbc:neo4j:neo4j+ssc://host:port` is supported
- ❌ `jdbc:neo4j:http://host:port` is NOT supported
- ❌ `jdbc:neo4j:https://host:port` is NOT supported
- ❌ `jdbc:neo4j:file:///path/to/neo4j` is NOT supported

Read more about what each URI scheme means in the [Neo4j driver manual](https://neo4j.com/docs/java-manual/current/client-applications/#_examples).

## JDBC connectivity

For versions 4.18.0.1 and earlier, the Neo4j extension relied on a [third-party JDBC connector](https://github.com/neo4j-contrib/neo4j-jdbc).
Configuration parameters for this connector can be found in [this link](https://github.com/neo4j/neo4j-jdbc/tree/5.0?tab=readme-ov-file#configuration).

Starting from version 4.19.0, the extension defines its own built-in JDBC connector.

The built-in connector is simpler and easier to reason about because Liquibase only needs one connection for a single execution and relies on a specific, finite set of JDBC APIs.

This new connector has a few key differences from the third-party connector.

The serialization and deserialization logic of the built-in connector is stricter.

!!! warning
    [This issue](https://github.com/neo4j-contrib/neo4j-jdbc/issues/412) may arise if you try to use the third-party connector with recent versions of the extension.

Since it is tailored towards Liquibase use only, the new connector is not registered through the standard JDBC mechanisms.

```java
import java.sql.Connection;
import java.sql.DriverManager;
// [...]
Connection connection = DriverManager.getConnection("jdbc:neo4j:neo4j://localhost", "neo4j", "<redacted>");
// this will not work! see below
```

If you need access to a JDBC `Connection` instance to programmatically configure Liquibase, the code to run is as follows:

=== "Plain old Java"

    ```java
    import liquibase.ext.neo4j.database.jdbc.Neo4jDriver;

    import java.sql.Connection;
    import java.sql.SQLException;
    import java.util.Properties;
    // [...]
    Properties properties = new Properties();
    properties.setProperty("user", "neo4j");
    properties.setProperty("password", "<redacted>");
    Connection connection = new Neo4jDriver().connect(
        "jdbc:neo4j:neo4j://localhost",
        properties
    );
    ```

=== "Spring Boot (properties)"

    ```properties
    spring.liquibase.driver-class-name=liquibase.ext.neo4j.database.jdbc.Neo4jDriver
    spring.liquibase.url=jdbc:neo4j:bolt://localhost
    spring.liquibase.user=neo4j
    spring.liquibase.password=<redacted>
    ```

=== "Spring Boot (Java Config)"

    ```java
    import javax.sql.DataSource;
    import liquibase.ext.neo4j.database.jdbc.Neo4jDriver;
    import org.springframework.boot.autoconfigure.liquibase.LiquibaseDataSource;
    import org.springframework.context.annotation.Bean;
    import org.springframework.jdbc.datasource.SimpleDriverDataSource;
    // [...]

    @LiquibaseDataSource
    @Bean
    public DataSource liquibaseNeo4jDataSource() {
        // SimpleDriverDataSource is a great fit for Liquibase
        // since it does not need any connection pooling
        // note: you can set properties instead of hardcoding credentials
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource(
            new Neo4jDriver(),
            "jdbc:neo4j:bolt://localhost"
        );
        dataSource.setUsername("neo4j");
        dataSource.setPassword("<redacted>");
        return dataSource;
    }
    ```

#### Configuration

You can fine-tune the behavior of the JDBC driver or the underlying Java Bolt driver by configuring any of the settings
below.

!!! important
    Some settings supported by the third-party JDBC connector are not supported yet. If you happen to need one of them, please
    [open an issue](https://github.com/liquibase/liquibase-neo4j/issues/new) and describe why it is needed in your situation.

| Setting                             | Description                                                                                                                   | Allowed values                                                                                                       | URL? | Properties? | Remarks                                                                                                                                                                                             |
|:------------------------------------|:------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------|:-----|:------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `database`                          | Sets database name                                                                                                            | Any valid database name                                                                                              | yes  | yes         | This setting is only available for Neo4j 4+ servers                                                                                                                                                 |
| `nossl`                             | Disables encryption                                                                                                           | - [empty]<br/>- "true"                                                                                               | yes  | yes         | "false" has no effect.<br/>Using this setting in conjunction with `encryption` or with an incompatible URI scheme is unspecified.<br/>Favour the appropriate URL scheme instead (`bolt` or `neo4j`) |
| `encryption`                        | Sets whether the traffic should be encrypted                                                                                  | - [empty]<br/>- "true"<br/>- "false"                                                                                 | yes  | yes         | Using this setting in conjunction with `nossl` is unspecified. Favour the appropriate URL scheme instead                                                                                            |
| `trust.strategy`                    | Configures how the underlying [driver](https://github.com/neo4j/neo4j-java-driver) handles certificates                       | - "TRUST_ALL_CERTIFICATES" <br/>- "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES" <br/>- "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES" | yes  | yes         | "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES" requires the setting "trusted.certificate.file"                                                                                                               |
| `trusted.certificate.file`          | Custom certificate file name                                                                                                  | A valid path                                                                                                         | yes  | yes         | Only used in combination with the "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES" trust strategy                                                                                                              |
| `connection.acquisition.timeout`    | Sets the maximum amount of time it takes for the full connection pool to acquire a connection                                 | Any integer value                                                                                                    | yes  | yes         | The value is in milliseconds.                                                                                                                                                                       |
| `connection.liveness.check.timeout` | Sets the threshold after which idle connections are tested for liveness before they are acquired again                        | Any integer value                                                                                                    | yes  | yes         | The value is in minutes.                                                                                                                                                                            |
| `connection.timeout`                | Sets the socket connection timeout                                                                                            | Any positive integer value                                                                                           | yes  | yes         |                                                                                                                                                                                                     |
| `leaked.sessions.logging`           | Enables leaked sessions logging (i.e. sessions not properly closed, leaking underlying connections leading to OOM)            | - [empty]<br/>- "true"<br/>                                                                                          | yes  | yes         | "false" has no effect                                                                                                                                                                               |
| `max.connection.lifetime`           | Sets the threshold after which a connection is not acquired again                                                             | Any integer value                                                                                                    | yes  | yes         | The value is in milliseconds                                                                                                                                                                        |
| `max.connection.poolsize`           | Sets the maximum size of the underlying [driver](https://github.com/neo4j/neo4j-java-driver)'s connection pool                | Any integer value                                                                                                    | yes  | yes         | The extension now overrides the driver's default pool size to 1 (since `4.22.0.1`)                                                                                                                  |
| `max.transaction.retry.time`        | Sets the maximum time transaction can be retried                                                                              | Any positive integer value                                                                                           | yes  | yes         | The value is in milliseconds.<br/>This setting is currently ineffective since the purpose-built JDBC connector does not rely on transaction functions                                               |
| `fetch.size`                        | Sets the number of records per batch to fetch                                                                                 | -1 or any strictly positive integer value                                                                            | yes  | yes         | This setting is only available for Neo4j 4+ servers<br/>-1 disables batching (and lead to memory issues, use with caution)                                                                          |
| `impersonated.user`                 | Sets the username to impersonate                                                                                              | Any valid username                                                                                                   | yes  | yes         | This setting is only available for Neo4j 4.4+ servers                                                                                                                                               |
| `driver.logging.console.level`      | Sets the `java.util.logging.Level` by name or numeric value and redirects the driver logs to the standard error output stream | Any valid level name or numeric value                                                                                | yes  | yes         | This setting is available since version `4.22.0.1` included                                                                                                                                         |
| `driver.logging.jul.level`          | Sets the `java.util.logging.Level` by name or numeric value to use by the built-in Java Util Logging system                   | Any valid level name or numeric value                                                                                | yes  | yes         | This setting is available since version `4.22.0.1` included                                                                                                                                         |
| `driver.logging.slf4j`              | Sets the driver logging to SLF4J                                                                                              | - [empty]<br/>- "true"<br/>                                                                                          | yes  | yes         | This setting is available since version `4.22.0.1` included                                                                                                                                         |
| `driver.logging.none`               | Disables driver logging                                                                                                       | - [empty]<br/>- "true"<br/>                                                                                          | yes  | yes         | This setting is available since version `4.22.0.1` included                                                                                                                                         |

!!! important
    - Setting names are normalized to lower case (per the English locale case rules).
    - If a setting is expressed both via URL and properties, the setting specified by URL has higher precedence.
    - URL settings without value are considered equivalent to boolean flags with value `true`.

{! include-markdown 'includes/_abbreviations.md' !}
