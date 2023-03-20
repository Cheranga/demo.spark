using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

public abstract class LoanSchema : ISchema
{
    public IntegerDataType Id => null!;
    public StringDataType Name => null!;
    public IntegerDataType StudentId => null!;
}