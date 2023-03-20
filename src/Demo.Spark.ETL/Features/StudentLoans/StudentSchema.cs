using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.StudentLoans;

public abstract class StudentSchema : ISchema
{
    public IntegerDataType Id  { get; }
    public StringDataType Name  { get; }
    public IntegerDataType LoanId  { get; }
}