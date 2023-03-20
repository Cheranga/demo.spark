using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.ETL.Core;

public interface ISparkDotNetType<out TSpark, out TDotNet> where TSpark:DataType
{
}

public abstract class StringDataType : ISparkDotNetType<StringType, string>
{
}

public abstract class IntegerDataType : ISparkDotNetType<IntegerType, int>
{
}