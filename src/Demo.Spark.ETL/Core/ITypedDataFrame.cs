using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Core;

public interface ISchema { }

public interface ITypedDataFrame<T>
    where T : ISchema
{
    DataFrame ToRaw();
}

