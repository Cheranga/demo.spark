using System.Linq.Expressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Demo.Spark.ETL.Core;

public interface ISchema { }

public class StudentSchema : ISchema
{
    public IntegerType Id { get; }
    public StringType Name { get; }
}

public interface ITypedDataFrame<T>
    where T : ISchema { }

public static class ExpressionTreeHelpers
{
    public static string Col<TSchema>(this Expression<Func<TSchema, DataType>> exp)
        where TSchema : ISchema
    {
        if (exp.Body is not MemberExpression memberExp)
            memberExp = ((exp.Body as UnaryExpression)!.Operand as MemberExpression)!;
        return memberExp.Member.Name;
    }
}

public class StudentsDataFrame : ITypedDataFrame<StudentSchema>
{
    private readonly DataFrame _dataFrame;

    public StudentsDataFrame(DataFrame dataFrame) => _dataFrame = dataFrame;

    public Column Col(Expression<Func<StudentSchema, DataType>> expression) =>
        _dataFrame.Col(expression.Col());

    public StudentsDataFrame FindStudentById(int studentId) =>
        new(_dataFrame.Where(Col(x => x.Id).EqualTo(Lit(studentId))));

    public StudentsDataFrame FindStudentByName(string name) =>
        new(_dataFrame.Where(Upper(Col(x => x.Name)).EqualTo(Upper(Lit(name)))));

    public StudentsDataFrame Select(params Expression<Func<StudentSchema, DataType>>[] cols)
    {
        var columns = cols.Select(Col);
        return new StudentsDataFrame(_dataFrame.Select(columns.ToArray()));
    }

    public StudentsDataFrame Filter(Expression<Func<StudentSchema, DataType>> expr, object value) =>
        new(_dataFrame.Where(Col(expr).EqualTo(Lit(value))));

    public DataFrame ToDataFrame() => _dataFrame;
}
