package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Config
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.internal.retry.RetrySettings
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES
import static org.neo4j.driver.Config.TrustStrategy.trustAllCertificates
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy
import static org.neo4j.driver.Config.TrustStrategy.trustSystemCertificates

class DriverConfigSupplierTest extends Specification {

    def "creates the driver configuration without encryption"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.encrypted() == config.encrypted()

        where:
        queryString               | properties              | config
        ["noSsl": ["true"]]       | [:]                     | Config.builder().withoutEncryption().build()
        ["nossl": ["true"]]       | [:]                     | Config.builder().withoutEncryption().build()
        [:]                       | ["noSsl": "true"]       | Config.builder().withoutEncryption().build()
        [:]                       | ["nossl": "true"]       | Config.builder().withoutEncryption().build()
        ["nossl": ["true"]]       | ["nossl": "false"]      | Config.builder().withoutEncryption().build()
        ["encryption": ["false"]] | [:]                     | Config.builder().withoutEncryption().build()
        [:]                       | ["encryption": "false"] | Config.builder().withoutEncryption().build()
        ["encryption": ["false"]] | ["encryption": "true"]  | Config.builder().withoutEncryption().build()

    }

    def "creates the driver configuration with encryption"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.encrypted() == config.encrypted()

        where:
        queryString              | properties              | config
        ["encryption": ["true"]] | [:]                     | Config.builder().withEncryption().build()
        [:]                      | ["encryption": "true"]  | Config.builder().withEncryption().build()
        ["encryption": ["true"]] | ["encryption": "false"] | Config.builder().withEncryption().build()

    }

    def "creates the driver configuration with a custom trust strategy"() {
        expect:
        def expectedConfig = config.trustStrategy()
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        verifyAll(actualConfig.trustStrategy()) {
            certFiles() == expectedConfig.certFiles()
            revocationCheckingStrategy() == expectedConfig.revocationCheckingStrategy()
            hostnameVerificationEnabled == expectedConfig.hostnameVerificationEnabled
        }

        where:
        queryString                                                   | properties                                                  | config
        ["trust.strategy": ["TRUST_ALL_CERTIFICATES"]]                | [:]                                                         | Config.builder().withTrustStrategy(trustAllCertificates()).build()
        [:]                                                           | ["trust.strategy": "TRUST_ALL_CERTIFICATES"]                | Config.builder().withTrustStrategy(trustAllCertificates()).build()
        ["trust.strategy": ["TRUST_ALL_CERTIFICATES"]]                | ["trust.strategy": "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"]   | Config.builder().withTrustStrategy(trustAllCertificates()).build()
        ["trust.strategy": ["TRUST_ALL_CERTIFICATES"]]                | ["trust.strategy": "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]   | Config.builder().withTrustStrategy(trustAllCertificates()).build()
        ["trust.strategy"          : ["TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"],
         "trusted.certificate.file": ["some.file"]]                   | [:]                                                         | Config.builder().withTrustStrategy(trustCustomCertificateSignedBy(new File("some.file"))).build()
        [:]                                                           | ["trust.strategy"          : "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
                                                                         "trusted.certificate.file": "some.file"]                   | Config.builder().withTrustStrategy(trustCustomCertificateSignedBy(new File("some.file"))).build()
        ["trust.strategy"          : ["TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"],
         "trusted.certificate.file": ["some.file"]]                   | ["trust.strategy": "TRUST_ALL_CERTIFICATES"]                | Config.builder().withTrustStrategy(trustCustomCertificateSignedBy(new File("some.file"))).build()
        ["trust.strategy"          : ["TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"],
         "trusted.certificate.file": ["some.file"]]                   | ["trust.strategy": "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]   | Config.builder().withTrustStrategy(trustCustomCertificateSignedBy(new File("some.file"))).build()
        ["trust.strategy": ["TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]]   | [:]                                                         | Config.builder().withTrustStrategy(trustSystemCertificates()).build()
        [:]                                                           | ["trust.strategy": "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]   | Config.builder().withTrustStrategy(trustSystemCertificates()).build()
        ["trust.strategy": ["TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]]   | ["trust.strategy": "TRUST_ALL_CERTIFICATES"]                | Config.builder().withTrustStrategy(trustSystemCertificates()).build()
        ["trust.strategy": ["TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]]   | ["trust.strategy": "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES"]   | Config.builder().withTrustStrategy(trustSystemCertificates()).build()
    }

    def "creates the driver configuration with a custom connection acquisition timeout"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.connectionAcquisitionTimeoutMillis() == config.connectionAcquisitionTimeoutMillis()

        where:
        queryString                                | properties                               | config
        ["connection.acquisition.timeout": ["42"]] | [:]                                      | Config.builder().withConnectionAcquisitionTimeout(42, MILLISECONDS).build()
        [:]                                        | ["connection.acquisition.timeout": "42"] | Config.builder().withConnectionAcquisitionTimeout(42, MILLISECONDS).build()
        ["connection.acquisition.timeout": ["42"]] | ["connection.acquisition.timeout": "24"] | Config.builder().withConnectionAcquisitionTimeout(42, MILLISECONDS).build()
    }

    def "creates the driver configuration with a custom connection liveness check timeout"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.idleTimeBeforeConnectionTest() == config.idleTimeBeforeConnectionTest()

        where:
        queryString                                   | properties                                  | config
        ["connection.liveness.check.timeout": ["42"]] | [:]                                         | Config.builder().withConnectionLivenessCheckTimeout(42, MINUTES).build()
        [:]                                           | ["connection.liveness.check.timeout": "42"] | Config.builder().withConnectionLivenessCheckTimeout(42, MINUTES).build()
        ["connection.liveness.check.timeout": ["42"]] | ["connection.liveness.check.timeout": "24"] | Config.builder().withConnectionLivenessCheckTimeout(42, MINUTES).build()
    }

    def "creates the driver configuration with a custom connection timeout"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.connectionTimeoutMillis() == config.connectionTimeoutMillis()

        where:
        queryString                    | properties                   | config
        ["connection.timeout": ["42"]] | [:]                          | Config.builder().withConnectionTimeout(42, MILLISECONDS).build()
        [:]                            | ["connection.timeout": "42"] | Config.builder().withConnectionTimeout(42, MILLISECONDS).build()
        ["connection.timeout": ["42"]] | ["connection.timeout": "24"] | Config.builder().withConnectionTimeout(42, MILLISECONDS).build()
    }

    def "creates the driver configuration with leaked sessions logging"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.logLeakedSessions() == config.logLeakedSessions()

        where:
        queryString                           | properties                           | config
        ["leaked.sessions.logging": ["true"]] | [:]                                  | Config.builder().withLeakedSessionsLogging().build()
        [:]                                   | ["leaked.sessions.logging": "true"]  | Config.builder().withLeakedSessionsLogging().build()
        ["leaked.sessions.logging": ["true"]] | ["leaked.sessions.logging": "false"] | Config.builder().withLeakedSessionsLogging().build()
    }

    def "creates the driver configuration with a custom max connection lifetime"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.maxConnectionLifetimeMillis() == config.maxConnectionLifetimeMillis()

        where:
        queryString                         | properties                        | config
        ["max.connection.lifetime": ["42"]] | [:]                               | Config.builder().withMaxConnectionLifetime(42, MILLISECONDS).build()
        [:]                                 | ["max.connection.lifetime": "42"] | Config.builder().withMaxConnectionLifetime(42, MILLISECONDS).build()
        ["max.connection.lifetime": ["42"]] | ["max.connection.lifetime": "24"] | Config.builder().withMaxConnectionLifetime(42, MILLISECONDS).build()
    }

    def "creates the driver configuration with a custom max connection pool size"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.maxConnectionPoolSize() == config.maxConnectionPoolSize()

        where:
        queryString                         | properties                        | config
        ["max.connection.poolsize": ["42"]] | [:]                               | Config.builder().withMaxConnectionPoolSize(42).build()
        [:]                                 | ["max.connection.poolsize": "42"] | Config.builder().withMaxConnectionPoolSize(42).build()
        ["max.connection.poolsize": ["42"]] | ["max.connection.poolsize": "24"] | Config.builder().withMaxConnectionPoolSize(42).build()
    }

    def "creates the driver configuration with a custom max transaction retry time"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).get()
        actualConfig.maxTransactionRetryTimeMillis() == config.maxTransactionRetryTimeMillis()

        where:
        queryString                            | properties                           | config
        ["max.transaction.retry.time": ["42"]] | [:]                                  | Config.builder().withMaxTransactionRetryTime(42, MILLISECONDS).build()
        [:]                                    | ["max.transaction.retry.time": "42"] | Config.builder().withMaxTransactionRetryTime(42, MILLISECONDS).build()
        ["max.transaction.retry.time": ["42"]] | ["max.transaction.retry.time": "24"] | Config.builder().withMaxTransactionRetryTime(42, MILLISECONDS).build()
    }

    def "creates the session configuration with a custom database name"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).getSessionConfig()
        actualConfig.database().get() == config.database().get()

        where:
        queryString            | properties             | config
        ["database": ["mydb"]] | [:]                    | SessionConfig.builder().withDatabase("mydb").build()
        [:]                    | ["database": "mydb"]   | SessionConfig.builder().withDatabase("mydb").build()
        ["database": ["mydb"]] | ["database": "yourdb"] | SessionConfig.builder().withDatabase("mydb").build()
    }

    def "creates the session configuration with a custom fetch size"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).getSessionConfig()
        actualConfig.fetchSize().get() == config.fetchSize().get()

        where:
        queryString            | properties           | config
        ["fetch.size": ["42"]] | [:]                  | SessionConfig.builder().withFetchSize(42).build()
        [:]                    | ["fetch.size": "42"] | SessionConfig.builder().withFetchSize(42).build()
        ["fetch.size": ["42"]] | ["fetch.size": "24"] | SessionConfig.builder().withFetchSize(42).build()
    }

    def "creates the session configuration with a custom impersonated user"() {
        expect:
        def actualConfig = new DriverConfigSupplier(new QueryString(queryString), propertiesOf(properties)).getSessionConfig()
        actualConfig.impersonatedUser().get() == config.impersonatedUser().get()

        where:
        queryString                      | properties                       | config
        ["impersonated.user": ["selda"]] | [:]                              | SessionConfig.builder().withImpersonatedUser("selda").build()
        [:]                              | ["impersonated.user": "selda"]   | SessionConfig.builder().withImpersonatedUser("selda").build()
        ["impersonated.user": ["selda"]] | ["impersonated.user": "özdemir"] | SessionConfig.builder().withImpersonatedUser("selda").build()
    }

    def "creates the driver configuration with a custom user agent"() {
        given:
        def supplier = new DriverConfigSupplier(new QueryString(Collections.emptyMap()), new Properties())

        when:
        def config = supplier.get()

        then:
        config.userAgent().startsWith("liquibase-neo4j/")
    }

    Properties propertiesOf(Map<String, String> dictionary) {
        def props = new Properties()
        props.putAll(dictionary)
        return props
    }

    def <T> T fieldOf(Object object, String fieldName, Class<T> type) {
        def field = object.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        return type.cast(field.get(object))
    }
}
