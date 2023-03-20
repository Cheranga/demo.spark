using Demo.Spark.ETL.Core;
using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Features.StudentLoans;

public sealed class StudentLoansDataFrame : TypedDataFrameBase<StudentLoanSchema>
{
    public StudentLoansDataFrame(DataFrame dataFrame) : base(dataFrame)
    {
    }
}