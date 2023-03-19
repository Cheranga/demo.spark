using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.ETL.Core;

public static class SparkHelper
{
    public static Column GetCol(this DataFrame dataFrame, Func<DataFrame, Column> colFunc) =>
        colFunc(dataFrame);

    public static Box<DataFrame> GetDataFrameFor(DataFrame dataFrame) => new(dataFrame);

    public static DataType ToDataType(this Type type)
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

    public static StructType GetSchema(this object model)
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

    public static object GetPropertyValue(this PropertyInfo property, object instance) =>
        (Type.GetTypeCode(property.PropertyType) switch
        {
            TypeCode.DateTime => new Timestamp((DateTime)property.GetValue(instance)!),
            _ => property.GetValue(instance)
        })!;
}
