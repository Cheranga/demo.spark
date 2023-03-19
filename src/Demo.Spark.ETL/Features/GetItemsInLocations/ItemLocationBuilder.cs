using Microsoft.Spark.Sql;

namespace Demo.Spark.ETL.Features.GetItemsInLocations;

public static class ItemLocationBuilder
{
    public static DataFrame Build(DataFrame items, DataFrame itemLocations)
    {
        var combinedData = items.Join(
            itemLocations,
            items
                .Col("ItemNumber")
                .EqualTo(itemLocations.Col("ItemNumber"))
                .And(items.Col("LocationCode").EqualTo(itemLocations.Col("LocationCode")))
        );

        var transformedData = combinedData.Select(
            items.Col("ItemNumber"),
            itemLocations.Col("Country"),
            itemLocations.Col("LocationCode"),
            items.Col("LastModified")
        );
        return transformedData;
    }
}
