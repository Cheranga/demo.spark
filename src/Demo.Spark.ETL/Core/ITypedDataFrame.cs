namespace Demo.Spark.ETL.Core;

public interface ISchema { }

public interface ITypedDataFrame<T>
    where T : ISchema { }

