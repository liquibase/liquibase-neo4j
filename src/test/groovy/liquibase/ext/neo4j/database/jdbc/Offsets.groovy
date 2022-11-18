package liquibase.ext.neo4j.database.jdbc

class Offsets {

    static int currentUtcOffsetMillis() {
        return TimeZone.getDefault().getOffset(System.currentTimeMillis())
    }
}
