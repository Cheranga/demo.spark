namespace Demo.Spark.Tests;

public class SparkInitializer : IDisposable
{
    public SparkSession Spark { get; }

    public SparkInitializer()
    {
        Spark = SparkSession.Builder().AppName("Demo.Spark").GetOrCreate();
    }
    
    public void Dispose()
    {
        Spark.Dispose();
    }
}

[CollectionDefinition(Name)]
public class SparkTestCollection : ICollectionFixture<SparkInitializer>
{
    public const string Name = nameof(SparkTestCollection);
}