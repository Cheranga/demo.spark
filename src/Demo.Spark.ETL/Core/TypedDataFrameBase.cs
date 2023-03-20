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

    protected Column Col<TSpark, TDotNet>(
        Expression<Func<TSchema, ISparkDotNetType<TSpark, TDotNet>>> expression
    )
        where TSpark : DataType => DataFrame.Col(expression.Col());

    public DataFrame ToRaw() => DataFrame;
}