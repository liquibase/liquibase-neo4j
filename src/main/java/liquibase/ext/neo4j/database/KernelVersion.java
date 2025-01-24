package liquibase.ext.neo4j.database;

import java.util.Objects;

public class KernelVersion implements Comparable<KernelVersion> {

    public static final KernelVersion V3_5_0 = new KernelVersion(3, 5, 0);
    public static final KernelVersion V4_0_0 = new KernelVersion(4, 0, 0);
    public static final KernelVersion V4_3_0 = new KernelVersion(4, 3, 0);
    public static final KernelVersion V4_4_0 = new KernelVersion(4, 4, 0);
    public static final KernelVersion V5_0_0 = new KernelVersion(5, 0, 0);
    public static final KernelVersion V5_24_0 = new KernelVersion(5, 24, 0);
    public static final KernelVersion V5_26_0 = new KernelVersion(5, 26, 0);

    private final int major;
    private final int minor;
    private final int patch;

    public static KernelVersion parse(String version) {
        int major = -1;
        int minor = -1;
        int patch = -1;
        String buffer = "";
        for (char c : version.toCharArray()) {
            if (c != '.') {
                buffer += c;
                continue;
            }
            if (major == -1) {
                major = Integer.parseInt(buffer, 10);
            } else if (minor == -1) {
                minor = parseMinor(buffer);
            } else {
                throw invalidVersion(version);
            }
            buffer = "";
        }
        if (buffer.isEmpty()) {
            throw invalidVersion(version);
        }
        if (major == -1) {
            major = Integer.parseInt(buffer, 10);
        } else if (minor == -1) {
            minor = parseMinor(buffer);
        } else {
            patch = parsePatch(buffer);
        }

        if (minor == -1) {
            return new KernelVersion(major);
        }
        if (patch == -1) {
            return new KernelVersion(major, minor);
        }
        return new KernelVersion(major, minor, patch);
    }

    public KernelVersion(int major) {
        this(major, Integer.MAX_VALUE);
    }

    public KernelVersion(int major, int minor) {
        this(major, minor, Integer.MAX_VALUE);
    }

    public KernelVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    public int major() {
        return major;
    }

    public int minor() {
        return minor;
    }

    public int patch() {
        return patch;
    }

    @Override
    public int compareTo(KernelVersion other) {
        if (major != other.major) {
            return signum(major - other.major);
        }
        if (minor != other.minor) {
            return signum(minor - other.minor);
        }
        return signum(patch - other.patch);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KernelVersion)) return false;
        KernelVersion that = (KernelVersion) o;
        return major == that.major && minor == that.minor && patch == that.patch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch);
    }

    @Override
    public String toString() {
        return versionString();
    }

    public String versionString() {
        if (minor == Integer.MAX_VALUE) {
            return String.format("%d", major);
        }
        if (patch == Integer.MAX_VALUE) {
            return String.format("%d.%d", major, minor);
        }
        return String.format("%d.%d.%d", major, minor, patch);
    }

    private static int parseMinor(String buffer) {
        return Integer.parseInt(buffer.replace("-aura", ""), 10);
    }


    private static int parsePatch(String buffer) {
        int end = buffer.indexOf('-');
        if (end == -1) {
            end = buffer.length();
        }
        return Integer.parseInt(buffer.substring(0, end), 10);
    }

    private static int signum(int result) {
        return (int) Math.signum(result);
    }

    private static IllegalArgumentException invalidVersion(String version) {
        return new IllegalArgumentException(String.format("Invalid Neo4j version: %s", version));
    }
}
