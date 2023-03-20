using System.Linq.Expressions;
using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.Schemas;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Demo.Spark.ETL.Features.StudentLoans;

public sealed class StudentsDataFrame : TypedDataFrameBase<StudentSchema>
{
    public StudentsDataFrame(DataFrame dataFrame)
        : base(dataFrame) { }

    public StudentsDataFrame FindStudentById(int studentId) =>
        new(DataFrame.Where(this.Col(x => x.Id).EqualTo(Lit(studentId))));

    public StudentsDataFrame FindStudentByName(string name) =>
        new(DataFrame.Where(Upper(this.Col(x => x.Name)).EqualTo(Upper(Lit(name)))));

    public StudentsDataFrame Filter<TSpark, TDotNet>(
        Expression<Func<StudentSchema, ISparkDotNetType<TSpark, TDotNet>>> expr,
        TDotNet value
    )
        where TSpark : DataType => new(DataFrame.FilterDataFrame(expr, value));
}
