using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

public abstract class StudentSchema : ISchema
{
    public IntegerDataType Id => null!;
    public StringDataType Name => null!;
    public IntegerDataType LoanId => null!;
}