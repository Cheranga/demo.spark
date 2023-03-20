using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

public class StudentLoanSchema : ISchema
{
    public IntegerDataType CustomerId => null!;
    public StringDataType CustomerFullName => null!;
    public StringDataType BankLoanType => null!;
}