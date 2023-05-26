package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.Config.ConfigBuilder;
import org.neo4j.driver.Config.TrustStrategy;
import org.neo4j.driver.Config.TrustStrategy.Strategy;
import org.neo4j.driver.Logging;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.SessionConfig.Builder;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;

class DriverConfigSupplier implements Supplier<Config> {

    private final QueryString urlConfiguration;
    private final Properties configuration;

    public DriverConfigSupplier(QueryString urlConfiguration, Properties configuration) {
        this.urlConfiguration = normalize(urlConfiguration);
        this.configuration = normalize(configuration);
    }

    @Override
    public Config get() {
        ConfigBuilder builder = Config.builder();
        if (readSingleSetting("nossl", Boolean::parseBoolean).orElse(false)) {
            builder = builder.withoutEncryption();
        }
        Strategy strategy = readSingleSetting("trust.strategy", Strategy::valueOf).orElse(null);
        if (strategy != null) {
            builder = builder.withTrustStrategy(trustStrategy(strategy));
        }
        Long acquisitionTimeout = readSingleSetting("connection.acquisition.timeout", Long::parseLong).orElse(null);
        if (acquisitionTimeout != null) {
            builder = builder.withConnectionAcquisitionTimeout(acquisitionTimeout, TimeUnit.MILLISECONDS);
        }
        Long livenessCheckTimeout = readSingleSetting("connection.liveness.check.timeout", Long::parseLong).orElse(null);
        if (livenessCheckTimeout != null) {
            builder = builder.withConnectionLivenessCheckTimeout(livenessCheckTimeout, TimeUnit.MINUTES);
        }
        Long connectionTimeout = readSingleSetting("connection.timeout", Long::parseLong).orElse(null);
        if (connectionTimeout != null) {
            builder = builder.withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS);
        }
        Boolean encryption = readSingleSetting("encryption", Boolean::parseBoolean).orElse(null);
        if (encryption != null) {
            builder = encryption ? builder.withEncryption() : builder.withoutEncryption();
        }
        if (readSingleSetting("leaked.sessions.logging", Boolean::parseBoolean).orElse(false)) {
            builder = builder.withLeakedSessionsLogging();
        }
        Long maxConnectionLifetime = readSingleSetting("max.connection.lifetime", Long::parseLong).orElse(null);
        if (maxConnectionLifetime != null) {
            builder = builder.withMaxConnectionLifetime(maxConnectionLifetime, TimeUnit.MILLISECONDS);
        }
        Integer maxPoolSize = readSingleSetting("max.connection.poolsize", Integer::parseInt).orElse(null);
        if (maxPoolSize != null) {
            builder = builder.withMaxConnectionPoolSize(maxPoolSize);
        } else {
            builder = builder.withMaxConnectionPoolSize(1);
        }
        Long maxTransactionRetryTime = readSingleSetting("max.transaction.retry.time", Long::parseLong).orElse(null);
        if (maxTransactionRetryTime != null) {
            builder = builder.withMaxTransactionRetryTime(maxTransactionRetryTime, TimeUnit.MILLISECONDS);
        }
        Level consoleLoggingLevel = readSingleSetting("driver.logging.console.level", Level::parse).orElse(null);
        if (consoleLoggingLevel != null) {
            builder = builder.withLogging(Logging.console(consoleLoggingLevel));
        }
        Level julLoggingLevel = readSingleSetting("driver.logging.jul.level", Level::parse).orElse(null);
        if (julLoggingLevel != null) {
            builder = builder.withLogging(Logging.javaUtilLogging(julLoggingLevel));
        }
        if (readSingleSetting("driver.logging.slf4j", Boolean::parseBoolean).orElse(false)) {
            builder = builder.withLogging(Logging.slf4j());
        }
        if (readSingleSetting("driver.logging.none", Boolean::parseBoolean).orElse(false)) {
            builder = builder.withLogging(Logging.none());
        }
        return builder.build();
    }

    private TrustStrategy trustStrategy(Strategy strategy) {
        switch (strategy) {
            case TRUST_ALL_CERTIFICATES:
                return TrustStrategy.trustAllCertificates();
            case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                String key = "trusted.certificate.file";
                File certificate = readSingleSetting(key, File::new)
                        .orElseThrow(() -> new RuntimeException(String.format("could not find certificate file setting %s", key)));
                return TrustStrategy.trustCustomCertificateSignedBy(certificate);
            case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                return TrustStrategy.trustSystemCertificates();
            default:
                throw new RuntimeException(String.format("unsupported trust strategy: %s", strategy));
        }
    }

    public SessionConfig getSessionConfig() {
        Builder builder = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE);
        String database = readSingleSetting("database", Function.identity()).orElse(null);
        if (database != null) {
            builder = builder.withDatabase(database);
        }
        Long fetchSize = readSingleSetting("fetch.size", Long::parseLong).orElse(null);
        if (fetchSize != null) {
            builder = builder.withFetchSize(fetchSize);
        }
        String impersonatedUser = readSingleSetting("impersonated.user", Function.identity()).orElse(null);
        if (impersonatedUser != null) {
            builder = builder.withImpersonatedUser(impersonatedUser);
        }
        return builder.build();
    }

   private <T> Optional<T> readSingleSetting(String key, Function<String, T> mapper) {
        T value = urlConfiguration.getSingleOrDefault(key, mapper, null);
        if (value != null) {
            return Optional.of(value);
        }
        String rawValue = configuration.getProperty(key);
        if (rawValue == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mapper.apply(rawValue));
    }

    private static QueryString normalize(QueryString configuration) {
        Map<String, List<String>> values = new HashMap<>(configuration.size());
        configuration.forEach((k, v) -> {
            String key = k.toLowerCase(Locale.ENGLISH);
            values.put(key, v);
        });
        return new QueryString(values);
    }

    private static Properties normalize(Properties configuration) {
        Properties properties = new Properties();
        configuration.forEach((k, v) -> {
            String key = ((String) k).toLowerCase(Locale.ENGLISH);
            String value = (String) v;
            properties.setProperty(key, value);
        });
        return properties;
    }
}
