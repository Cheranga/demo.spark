using System.Diagnostics.CodeAnalysis;
using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

[ExcludeFromCodeCoverage]
public abstract class StudentLoanSchema : ISchema
{
    public IntegerDataType CustomerId => null!;
    public StringDataType CustomerFullName => null!;
    public StringDataType BankLoanType => null!;
    public DateTimeDataType LoanStartDate => null!;
    public DateTimeDataType LoanEndDate => null!;
    public BooleanDataType IsDue => null!;
}