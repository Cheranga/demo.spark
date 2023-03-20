using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Features.Schemas;

public record ItemLocationSchema : IItemLocationSchema
{
    public Func<DataFrame, Column> ItemNumber => frame => frame.Col(nameof(ItemNumber));
    public Func<DataFrame, Column> LocationCode => frame => frame.Col(nameof(LocationCode));
    public Func<DataFrame, Column> Country => frame => frame.Col(nameof(Country));
}