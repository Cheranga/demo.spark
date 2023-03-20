using System.Linq.Expressions;
using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.Schemas;
using FluentAssertions;
using Microsoft.Spark;

namespace Demo.Spark.Tests.Core;

[Collection(SparkTestCollection.Name)]
public class TypedDataFrameExtensionTests
{
    private readonly SparkSession _spark;

    public TypedDataFrameExtensionTests(SparkInitializer initializer)
    {
        _spark = initializer.Spark;
    }

    [Fact]
    public void InvalidDataFrames()
    {
        var studentSchema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType()),
                new StructField("LoanId", new IntegerType())
            }
        );

        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[]
            {
                new(new object[] { 1, "Cheranga", 100 }),
                new(new object[] { 1, "Cheranga", 200 })
            },
            studentSchema
        );

        var action = () => dataFrame.ToSchema<LoanSchema>();
        action.Should().Throw<Exception>().WithInnerException<JvmException>();
    }

    [Fact]
    public void ValidDataFrames()
    {
        var studentSchema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType()),
                new StructField("LoanId", new IntegerType())
            }
        );

        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[]
            {
                new(new object[] { 1, "Cheranga", 100 }),
                new(new object[] { 1, "Cheranga", 200 })
            },
            studentSchema
        );

        var action = () => dataFrame.ToSchema<StudentSchema>();
        action.Should().NotThrow();
    }

    [Fact]
    public void Filtering()
    {
        var studentSchema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType()),
                new StructField("LoanId", new IntegerType())
            }
        );

        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[]
            {
                new(new object[] { 1, "Cheranga", 100 }),
                new(new object[] { 1, "Cheranga", 200 })
            },
            studentSchema
        );

        var filtered = dataFrame.FilterDataFrame<StudentSchema, StringType, string>(
            x => x.Name,
            "Cheranga"
        );

        filtered.Collect().ToList().Count.Should().Be(2);
    }
    
    [Fact]
    public void GetColNameFromExpression()
    {
        Expression<Func<StudentSchema, ISparkDotNetType<IntegerType, int>>> expr = schema => schema.Id;
        expr.Col().Should().Be("Id");
    }
}
