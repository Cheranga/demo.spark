using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Features.GetItemsInLocations;
using Demo.Spark.Tests.Models;
using FluentAssertions;

namespace Demo.Spark.Tests.Features;

public class ItemLocationBuilderTests
{
    private const string IsoDateFormat = "yyyy-MM-dd HH:mm:ss";
    private readonly SparkSession _spark;

    public ItemLocationBuilderTests() =>
        _spark = SparkSession.Builder().AppName(nameof(ItemLocationBuilderTests)).GetOrCreate();

    [Fact(DisplayName = "There are items which have matching item locations")]
    public void Test()
    {
        var currentDateTime = DateTime.UtcNow;
        var items = _spark.GetDataFrame(new[] { new Item(1000001, 2010, currentDateTime) });

        var itemLocations = _spark.GetDataFrame(
            new[] { new ItemLocation(1000001, 2010, "AU"), new ItemLocation(1000002, 2011, "NZ") }
        );

        var dataFrame = LooselyTypedItemLocationBuilder.Build(items, itemLocations);
        var results = dataFrame.Collect().ToList();
        results.Should().ContainSingle();

        var data = results.First();
        data.GetAs<int>("ItemNumber").Should().Be(1000001);
        data.GetAs<int>("LocationCode").Should().Be(2010);
        data.GetAs<string>("Country").Should().Be("AU");
        data.GetAs<Timestamp>("LastModified")
            .ToDateTime()
            .ToString(IsoDateFormat)
            .Should()
            .Be(currentDateTime.ToString(IsoDateFormat));
    }
}
