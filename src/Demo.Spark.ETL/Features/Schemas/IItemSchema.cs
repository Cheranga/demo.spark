using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Features.Schemas;

public interface IItemSchema
{
    Func<DataFrame, Column> ItemNumber { get; }

    Func<DataFrame, Column> LocationCode { get; }
    Func<DataFrame, Column> LastModified { get; }
}