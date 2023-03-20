using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.StudentLoans;

public class StudentLoanSchema : ISchema
{
    public IntegerDataType CustomerId  { get; }
    public StringDataType CustomerFullName { get; }
    public StringDataType BankLoanType { get; }
}