using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Features.Schemas;

[ExcludeFromCodeCoverage]
public record ItemSchema : IItemSchema
{
    public Func<DataFrame, Column> ItemNumber => frame => frame.Col(nameof(ItemNumber));
    public Func<DataFrame, Column> LocationCode => frame => frame.Col(nameof(LocationCode));
    public Func<DataFrame, Column> LastModified => frame => frame.Col(nameof(LastModified));
}