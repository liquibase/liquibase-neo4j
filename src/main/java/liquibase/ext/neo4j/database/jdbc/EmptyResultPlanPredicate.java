package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.summary.Plan;

import java.util.Locale;
import java.util.function.Predicate;

enum EmptyResultPlanPredicate implements Predicate<Plan> {

    HAS_EMPTY_RESULT;

    @Override
    public boolean test(Plan plan) {
        return plan
                .children()
                .stream()
                .map(Plan::operatorType)
                .noneMatch(o -> o.toUpperCase(Locale.ENGLISH).contains("EMPTYRESULT"));
    }
}
