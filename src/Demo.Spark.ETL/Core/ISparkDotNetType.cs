using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.ETL.Core;

public interface ISparkDotNetType<out TSpark, out TDotNet> where TSpark:DataType
{
}

[ExcludeFromCodeCoverage]
public abstract class StringDataType : ISparkDotNetType<StringType, string>
{
    public StringType SparkType => new();
    public string DotNetType => string.Empty;
}

[ExcludeFromCodeCoverage]
public abstract class IntegerDataType : ISparkDotNetType<IntegerType, int>
{
    public IntegerType SparkType => new();
    public int DotNetType => 666;
}