using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.GetItemsInLocations;
using Demo.Spark.Tests.Models;
using FluentAssertions;

namespace Demo.Spark.Tests.Features;

[Collection(SparkTestCollection.Name)]
public class StronglyTypedItemLocationBuilderTests
{
    private const string IsoDateFormat = "yyyy-MM-dd HH:mm:ss";
    private readonly SparkInitializer _env;

    public StronglyTypedItemLocationBuilderTests(SparkInitializer env) => _env = env;

    [Fact(DisplayName = "There are items which have matching item locations")]
    public void ItemLocationsAreAvailableForItems()
    {
        var currentDateTime = DateTime.UtcNow;
        var items = _env.Spark.GetDataFrame(new[] { new Item(1000001, 2010, currentDateTime) });

        var itemLocations = _env.Spark.GetDataFrame(
            new[] { new ItemLocation(1000001, 2010, "AU"), new ItemLocation(1000002, 2011, "NZ") }
        );

        var dataFrame = StronglyTypedItemLocationBuilder.Build(
            items,
            itemLocations,
            new ItemSchema(),
            new ItemLocationSchema()
        );
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
