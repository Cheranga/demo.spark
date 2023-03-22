namespace Demo.Spark.ETL.Extensions;

public static class ReflectionExtensions
{
    public static bool IsImplementing(this Type type, Type targetType) =>
        type.GetInterfaces()
            .Any(
                x =>
                    targetType.IsGenericType
                        ? x.IsGenericType && targetType.IsAssignableFrom(x.GetGenericTypeDefinition())
                        : targetType.IsAssignableFrom(x)
            );
}
