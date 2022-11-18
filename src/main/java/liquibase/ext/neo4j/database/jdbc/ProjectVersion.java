package liquibase.ext.neo4j.database.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

class ProjectVersion {

    private final String raw;
    private final int major;
    private final int minor;

    private ProjectVersion(String raw, int major, int minor) {
        this.raw = raw;
        this.major = major;
        this.minor = minor;
    }

    public static ProjectVersion parse() {
        String version = readProjectVersionFile();
        int minLength = "x.x.x".length();
        if (version == null || version.trim().length() < minLength) {
            throw new IllegalStateException(String.format("expected version %s to be at least %d characters long", version, minLength));
        }
        String[] parts = version.split("\\.");
        int majorVersion = Integer.parseInt(parts[0], 10);
        int minorVersion = Integer.parseInt(parts[1], 10);
        return new ProjectVersion(version, majorVersion, minorVersion);
    }

    public String getRaw() {
        return raw;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProjectVersion that = (ProjectVersion) o;
        return Objects.equals(raw, that.raw);
    }

    @Override
    public int hashCode() {
        return Objects.hash(raw);
    }

    @Override
    public String toString() {
        return "ProjectVersion{" +
                "rawVersion='" + raw + '\'' +
                '}';
    }

    private static String readProjectVersionFile() {
        try (InputStream resource = ProjectVersion.class.getResourceAsStream("/project.version")) {
            if (resource == null) {
                throw new RuntimeException("could not find version file to parse the project version from");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource))) {
                return reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("could not parse project version", e);
        }
    }

}
