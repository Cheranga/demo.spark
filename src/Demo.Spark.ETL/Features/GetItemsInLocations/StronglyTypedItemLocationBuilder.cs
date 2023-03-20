using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.Schemas;
using Microsoft.Spark.Sql;
using static Demo.Spark.ETL.Extensions.SparkExtensions;

namespace Demo.Spark.ETL.Features.GetItemsInLocations;

public static class StronglyTypedItemLocationBuilder
{
    public static DataFrame Build(
        DataFrame itemDataFrame,
        DataFrame itemLocationsDataFrame,
        IItemSchema itemSchema,
        IItemLocationSchema itemLocationSchema
    ) =>
        (
            from items in GetDataFrameFor(itemDataFrame)
            from itemLocations in GetDataFrameFor(itemLocationsDataFrame)
            from itemsWithItemLocations in GetItemsWhichAreAvailableInItemLocations(
                items,
                itemLocations,
                itemSchema,
                itemLocationSchema
            )
            from transformedDf in GetTransformedData(
                items,
                itemLocations,
                itemsWithItemLocations,
                itemSchema,
                itemLocationSchema
            )
            select transformedDf
        ).Data;

    private static Box<DataFrame> GetTransformedData(
        DataFrame items,
        DataFrame itemLocations,
        DataFrame combinedDataFrame,
        IItemSchema itemSchema,
        IItemLocationSchema itemLocationSchema
    ) =>
        combinedDataFrame
            .Select(
                itemSchema.ItemNumber(items),
                itemLocationSchema.Country(itemLocations),
                itemLocationSchema.LocationCode(itemLocations),
                itemSchema.LastModified(items)
            )
            .ToBox();

    private static Box<DataFrame> GetItemsWhichAreAvailableInItemLocations(
        DataFrame items,
        DataFrame itemLocations,
        IItemSchema itemSchema,
        IItemLocationSchema itemLocationSchema
    ) =>
        items
            .Join(
                itemLocations,
                itemSchema
                    .ItemNumber(items)
                    .EqualTo(itemLocationSchema.ItemNumber(itemLocations))
                    .And(
                        itemSchema
                            .LocationCode(items)
                            .EqualTo(itemLocationSchema.LocationCode(itemLocations))
                    )
            )
            .ToBox();
}
