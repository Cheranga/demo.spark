using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.StudentLoans;

public abstract class LoanSchema : ISchema
{
    public IntegerDataType Id { get; }
    public StringDataType Name  { get; }
    public IntegerDataType StudentId  { get; }
}