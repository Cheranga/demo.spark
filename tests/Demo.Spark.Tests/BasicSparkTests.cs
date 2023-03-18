using FluentAssertions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Demo.Spark.Tests;

public class BasicSparkTests : IDisposable
{
    private readonly SparkSession _spark;

    public BasicSparkTests() =>
        _spark = SparkSession.Builder().AppName(nameof(BasicSparkTests)).GetOrCreate();

    public void Dispose() => _spark.Dispose();

    [Fact]
    public void StartEndSparkSession()
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        var data = new[]
        {
            new GenericRow(new object[] {1, "A"}),
            new GenericRow(new object[] {2, "B"}),
        };

        var dataFrame = _spark.CreateDataFrame(data, schema);
        dataFrame.Collect().ToList().Count.Should().Be(2);
    }
}
