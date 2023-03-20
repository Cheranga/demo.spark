using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Core;

public abstract class TypedDataFrameBase<TSchema> : ITypedDataFrame<TSchema>
    where TSchema : ISchema
{
    protected TypedDataFrameBase(DataFrame dataFrame) => DataFrame = dataFrame;

    protected DataFrame DataFrame { get; }

    public DataFrame ToRaw() => DataFrame;

    public DataFrame Join<TDataFrame, TAnotherSchema>(
        TDataFrame joinWith,
        Column joinExpression,
        string joinType = "inner"
    )
        where TDataFrame : ITypedDataFrame<TAnotherSchema>
        where TAnotherSchema : ISchema =>
        DataFrame.Join(joinWith.ToRaw(), joinExpression, joinType);
}
