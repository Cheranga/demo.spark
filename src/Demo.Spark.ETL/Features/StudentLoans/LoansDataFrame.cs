﻿using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.Schemas;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Demo.Spark.ETL.Features.StudentLoans;

public sealed class LoansDataFrame : TypedDataFrameBase<LoanSchema>
{
    public LoansDataFrame(DataFrame dataFrame)
        : base(dataFrame) { }

    public LoansDataFrame GetLoan(int loanId) =>
        new(DataFrame.Where(this.Col(x => x.Id).EqualTo(Lit(loanId))));

    public LoansDataFrame GetLoan(string loanName) =>
        new(DataFrame.Where(Upper(this.Col(x => x.Name)).EqualTo(Upper(Lit(loanName)))));
}
