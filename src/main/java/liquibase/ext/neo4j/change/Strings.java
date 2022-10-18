package liquibase.ext.neo4j.change;

class Strings {

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }
}
