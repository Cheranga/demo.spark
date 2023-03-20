using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Microsoft.Spark.Sql;
using static Demo.Spark.ETL.Extensions.SparkExtensions;

namespace Demo.Spark.ETL.Features.GetItemsInLocations;

public static class LooselyTypedItemLocationBuilder
{
    public static DataFrame Build(DataFrame items, DataFrame itemLocations) =>
        (
            from itemDf in GetDataFrameFor(items)
            from itemLocationDf in GetDataFrameFor(itemLocations)
            from selectedDf in GetItemsWhichHaveLocations(itemDf, itemLocationDf)
            from transformedDf in GetTransformedData(itemDf, itemLocationDf, selectedDf)
            select transformedDf
        ).Data;

    private static Box<DataFrame> GetItemsWhichHaveLocations(
        DataFrame items,
        DataFrame itemLocations
    ) =>
        items
            .Join(
                itemLocations,
                items
                    .Col("ItemNumber")
                    .EqualTo(itemLocations.Col("ItemNumber"))
                    .And(items.Col("LocationCode").EqualTo(itemLocations.Col("LocationCode")))
            )
            .ToBox();

    private static Box<DataFrame> GetTransformedData(
        DataFrame items,
        DataFrame itemLocations,
        DataFrame commonData
    ) =>
        commonData
            .Select(
                items.Col("ItemNumber"),
                itemLocations.Col("Country"),
                itemLocations.Col("LocationCode"),
                items.Col("LastModified")
            )
            .ToBox();
}
