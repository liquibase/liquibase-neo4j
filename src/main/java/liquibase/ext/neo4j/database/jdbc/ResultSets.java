package liquibase.ext.neo4j.database.jdbc;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;

class ResultSets {

    public static String rsTypeName(int resultSetType) {
        if (resultSetType == TYPE_FORWARD_ONLY) {
            return "TYPE_FORWARD_ONLY";
        }
        if (resultSetType == TYPE_SCROLL_INSENSITIVE) {
            return "TYPE_SCROLL_INSENSITIVE";
        }
        if (resultSetType == TYPE_SCROLL_SENSITIVE) {
            return "TYPE_SCROLL_SENSITIVE";
        }
        return unknownValue(resultSetType);
    }

    public static String rsConcurrencyName(int resultSetConcurrency) {
        if (resultSetConcurrency == CONCUR_READ_ONLY) {
            return "CONCUR_READ_ONLY";
        }
        if (resultSetConcurrency == CONCUR_UPDATABLE) {
            return "CONCUR_UPDATABLE";
        }
        return unknownValue(resultSetConcurrency);
    }

    public static String rsHoldabilityName(int resultSetHoldability) {
        if (resultSetHoldability == HOLD_CURSORS_OVER_COMMIT) {
            return "HOLD_CURSORS_OVER_COMMIT";
        }
        if (resultSetHoldability == CLOSE_CURSORS_AT_COMMIT) {
            return "CLOSE_CURSORS_AT_COMMIT";
        }
        return unknownValue(resultSetHoldability);
    }

    private static String unknownValue(int resultSetType) {
        return String.format("UNKNOWN (raw value: %d)", resultSetType);
    }
}
