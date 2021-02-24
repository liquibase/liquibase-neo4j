package liquibase.ext.neo4j

class ReflectionUtils {

    static void setField(String fieldName, Object instance, Object value) {
        def field = instance.getClass().getDeclaredField(fieldName)
        field.setAccessible(true)
        field.set(instance, value)
    }

    static Object getField(String fieldName, Object instance) {
        def field = instance.getClass().getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(instance)
    }
}
