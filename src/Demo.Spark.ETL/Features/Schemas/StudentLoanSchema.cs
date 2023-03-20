﻿using System.Diagnostics.CodeAnalysis;
using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Features.Schemas;

[ExcludeFromCodeCoverage]
public class StudentLoanSchema : ISchema
{
    public IntegerDataType CustomerId => null!;
    public StringDataType CustomerFullName => null!;
    public StringDataType BankLoanType => null!;
    public DateTimeDataType StartDate => null!;
    public DateTimeDataType EndDate => null!;
}