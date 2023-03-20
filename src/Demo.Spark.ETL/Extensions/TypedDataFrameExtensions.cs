using System.Linq.Expressions;
using System.Reflection;
using Demo.Spark.ETL.Core;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Demo.Spark.ETL.Extensions;

public static class TypedDataFrameExtensions
{
    public static string Col<TSchema, TSpark, TDotNet>(
        this Expression<Func<TSchema, ISparkDotNetType<TSpark, TDotNet>>> exp
    )
        where TSpark : DataType
        where TSchema : ISchema
    {
        if (exp.Body is not MemberExpression memberExp)
            throw new Exception("Invalid expression");

        return memberExp.Member.Name;
    }

    public static string Col<TSchema>(this Expression<Func<TSchema, DataType>> exp)
        where TSchema : ISchema
    {
        if (exp.Body is not MemberExpression memberExp)
            memberExp = ((exp.Body as UnaryExpression)!.Operand as MemberExpression)!;
        return memberExp.Member.Name;
    }

    public static DataFrame FilterDataFrame<TSchema, TSpark, TDotNet>(
        this DataFrame dataFrame,
        Expression<Func<TSchema, ISparkDotNetType<TSpark, TDotNet>>> expr,
        TDotNet value
    )
        where TSchema : ISchema
        where TSpark : DataType => dataFrame.Filter(dataFrame.Col(expr.Col()).EqualTo(Lit(value)));

    public static DataFrame ToSchema<TSchema>(this DataFrame dataFrame)
        where TSchema : ISchema
    {
        var properties = typeof(TSchema).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var columns = properties.Select(x => dataFrame.Col(x.Name));
        return dataFrame.Select(columns.ToArray());
    }
}
