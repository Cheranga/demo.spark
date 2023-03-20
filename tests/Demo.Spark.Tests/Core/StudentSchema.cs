using Demo.Spark.ETL.Core;

namespace Demo.Spark.Tests.Core;

public abstract class StudentSchema : ISchema
{
    public readonly IntegerDataType Id = null!;
    public readonly StringDataType Name = null!;
}