package liquibase.ext.neo4j.database;

import java.util.Objects;

public class KernelVersion implements Comparable<KernelVersion> {

    public static final KernelVersion V3_5 = new KernelVersion(3, 5, 0);
    public static final KernelVersion V4_0 = new KernelVersion(4, 0, 0);
    public static final KernelVersion V4_3 = new KernelVersion(4, 3, 0);
    public static final KernelVersion V4_4 = new KernelVersion(4, 4, 0);
    public static final KernelVersion V5_0 = new KernelVersion(5, 0, 0);

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
            }
            buffer = "";
        }
        if (!buffer.isEmpty()) {
            if (major == -1) {
                major = Integer.parseInt(buffer, 10);
            } else if (minor == -1) {
                minor = parseMinor(buffer);
            } else {
                patch = Integer.parseInt(buffer, 10);
            }
        }
        if (major == -1) {
            throw new IllegalArgumentException(String.format("Invalid Neo4j version: %s", version));
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

    private static int signum(int result) {
        return (int) Math.signum(result);
    }

    private static int parseMinor(String buffer) {
        return Integer.parseInt(buffer.replace("-aura", ""), 10);
    }
}
