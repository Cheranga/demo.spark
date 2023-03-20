using System.Linq.Expressions;
using Demo.Spark.ETL.Extensions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.ETL.Core;

public abstract class TypedDataFrameBase<TSchema> : ITypedDataFrame<TSchema>
    where TSchema : ISchema
{
    protected DataFrame DataFrame { get; }

    protected TypedDataFrameBase(DataFrame dataFrame) => DataFrame = dataFrame;

    public Column Col<TSpark, TDotNet>(
        Expression<Func<TSchema, ISparkDotNetType<TSpark, TDotNet>>> expression
    )
        where TSpark : DataType => DataFrame.Col(expression.Col());
    
    public Column ColAs<TSpark, TDotNet>(
        Expression<Func<TSchema, ISparkDotNetType<TSpark, TDotNet>>> expression,
        string alias
    )
        where TSpark : DataType => DataFrame.Col(expression.Col()).As(alias);

    public DataFrame JoinWith<TDataFrame, TAnotherSchema>(TDataFrame joinWith, Column joinExpression) where TDataFrame : ITypedDataFrame<TAnotherSchema>
        where TAnotherSchema : ISchema =>
        DataFrame.Join(joinWith.ToRaw(), joinExpression);

    public DataFrame ToRaw() => DataFrame;
}