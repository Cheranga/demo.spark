using System.Linq.Expressions;
using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using static Microsoft.Spark.Sql.Functions;

namespace Demo.Spark.Tests.Core;

public sealed class StudentsDataFrame : TypedDataFrameBase<StudentSchema>
{
    public StudentsDataFrame FindStudentById(int studentId) =>
        new(DataFrame.Where(Col(x => x.Id).EqualTo(Lit(studentId))));

    public StudentsDataFrame FindStudentByName(string name) =>
        new(DataFrame.Where(Upper(Col(x => x.Name)).EqualTo(Upper(Lit(name)))));

    public StudentsDataFrame Filter<TSpark, TDotNet>(
        Expression<Func<StudentSchema, ISparkDotNetType<TSpark, TDotNet>>> expr,
        TDotNet value
    )
        where TSpark : DataType => new(DataFrame.FilterDataFrame(expr, value));

    public StudentsDataFrame(DataFrame dataFrame)
        : base(dataFrame) { }
}
