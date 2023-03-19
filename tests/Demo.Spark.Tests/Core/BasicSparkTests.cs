using FluentAssertions;

namespace Demo.Spark.Tests.Core;

[Collection(SparkTestCollection.Name)]
public class BasicSparkTests
{
    private readonly SparkInitializer _env;

    public BasicSparkTests(SparkInitializer env)
    {
        _env = env;
    }
    
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

        var dataFrame = _env.Spark.CreateDataFrame(data, schema);
        dataFrame.Collect().ToList().Count.Should().Be(2);
    }
}
