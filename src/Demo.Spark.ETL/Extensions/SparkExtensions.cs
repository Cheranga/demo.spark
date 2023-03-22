using System.Reflection;
using Demo.Spark.ETL.Core;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.ETL.Extensions;

public static class SparkExtensions
{
    public static Box<DataFrame> GetDataFrameFor(DataFrame dataFrame) =>
        Box<DataFrame>.New(dataFrame);

    private static DataType ToDataType(this Type type)
    {
        var typeCode = Type.GetTypeCode(type);
        return typeCode switch
        {
            TypeCode.Boolean => new BooleanType(),
            TypeCode.Int16 => new IntegerType(),
            TypeCode.Int32 => new IntegerType(),
            TypeCode.Int64 => new IntegerType(),
            TypeCode.Decimal => new DecimalType(),
            TypeCode.Double => new DoubleType(),
            TypeCode.String => new StringType(),
            TypeCode.DateTime => new TimestampType(),
            _ => throw new NotSupportedException()
        };
    }

    private static StructType GetSchema(this object model)
    {
        var properties = model.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var sparkProperties = properties.Select(
            x => new StructField(x.Name, x.PropertyType.ToDataType())
        );

        return new StructType(sparkProperties);
    }

    public static DataFrame GetDataFrame<TData>(this SparkSession spark, IEnumerable<TData> data)
    {
        var list = data.ToList();
        if (!list.Any())
            throw new Exception("there are no data to create a dataframe");

        var item = list.First();
        var metaData = new
        {
            properties = typeof(TData).GetProperties(BindingFlags.Instance | BindingFlags.Public),
            schema = item!.GetSchema()
        };

        var rows = list.Select(
            x => new GenericRow(metaData.properties.Select(p => p.GetPropertyValue(x!)).ToArray())
        );
        var dataFrame = spark.CreateDataFrame(rows, metaData.schema);
        return dataFrame;
    }

    private static object GetPropertyValue(this PropertyInfo property, object instance) =>
    (
        Type.GetTypeCode(property.PropertyType) switch
        {
            TypeCode.DateTime => new Timestamp((DateTime)property.GetValue(instance)!),
            _ => property.GetValue(instance)
        }
    )!;

    public static StructType ToSchema<TSchema>()
        where TSchema : ISchema
    {
        var properties = typeof(TSchema)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(x => x.PropertyType.IsImplementing(typeof(ISparkDotNetType<,>)))
            .Select(
                x =>
                    new
                    {
                        x.Name,
                        DotNetType = x.PropertyType
                            .GetInterface(typeof(ISparkDotNetType<,>).Name)!
                            .GenericTypeArguments[1]
                    }
            )
            .ToList();

        return new StructType(
            properties.Select(x => new StructField(x.Name, x.DotNetType.ToDataType()))
        );
    }
}