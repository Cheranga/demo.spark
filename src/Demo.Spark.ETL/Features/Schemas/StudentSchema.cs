using System.Diagnostics.CodeAnalysis;
using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

[ExcludeFromCodeCoverage]
public abstract class StudentSchema : ISchema
{
    public IntegerDataType Id => null!;
    public StringDataType Name => null!;
    public IntegerDataType LoanId => null!;
}