using Demo.Spark.ETL.Core;
using FluentAssertions;

namespace Demo.Spark.Tests.Core;

[Collection(SparkTestCollection.Name)]
public class TypedDataFrameTests
{
    private readonly SparkSession _spark;

    public TypedDataFrameTests(SparkInitializer initializer) => _spark = initializer.Spark;

    [Fact]
    public void SelectWithRawDataFrame()
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[] { new(new object[] { 1, "A" }), new(new object[] { 2, "B" }) },
            schema
        );

        var studentDataFrame = new StudentsDataFrame(dataFrame);

        var dataFrameWithId = dataFrame.Select(studentDataFrame.Col(s => s.Id));
        dataFrameWithId.Collect().ToList().Count.Should().Be(2);

        var dataFrameWithName = dataFrame.Select(studentDataFrame.Col(s => s.Name));
        dataFrameWithName.Collect().ToList().Count.Should().Be(2);

        var dataFrameWithAll = dataFrame.Select(
            studentDataFrame.Col(s => s.Id),
            studentDataFrame.Col(s => s.Name)
        );
        dataFrameWithAll.Collect().ToList().Count.Should().Be(2);

        var filteredByIdDataFrame = dataFrame.Filter(studentDataFrame.Col(s => s.Id).Gt(1));
        filteredByIdDataFrame.Collect().ToList().First().GetAs<int>("Id").Should().Be(2);
    }

    [Fact]
    public void SelectWithStudentDataFrame()
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[] { new(new object[] { 1, "A" }), new(new object[] { 2, "B" }) },
            schema
        );

        var studentDataFrame = new StudentsDataFrame(dataFrame);

        var dataFrameWithId = studentDataFrame.Select(x => x.Id);
        var dataFrameWithAll = studentDataFrame.Select(x => x.Id, x => x.Name);

        dataFrameWithId.ToDataFrame().Collect().ToList().Count.Should().Be(2);
        dataFrameWithAll.ToDataFrame().Collect().ToList().Count.Should().Be(2);
    }

    [Fact]
    public void FindStudentsWithStudentDataFrame()
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[] { new(new object[] { 1, "A" }), new(new object[] { 2, "B" }) },
            schema
        );

        var studentDataFrame = new StudentsDataFrame(dataFrame);

        var studentWithId = studentDataFrame.FindStudentById(1);
        var studentWithName = studentDataFrame.FindStudentByName("B");

        studentWithId.ToDataFrame().Collect().ToList().Count.Should().Be(1);
        studentWithName.ToDataFrame().Collect().ToList().Count.Should().Be(1);
    }

    [Fact]
    public void FilterStudentsWithStudentDataFrame()
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        var dataFrame = _spark.CreateDataFrame(
            new GenericRow[] { new(new object[] { 1, "A" }), new(new object[] { 2, "B" }) },
            schema
        );

        var studentDataFrame = new StudentsDataFrame(dataFrame);

        var studentWithId = studentDataFrame.Filter(x => x.Id, 1);
        var studentWithName = studentDataFrame.Filter(x => x.Name, "A");

        studentWithId.ToDataFrame().Collect().ToList().Count.Should().Be(1);
        studentWithName.ToDataFrame().Collect().ToList().Count.Should().Be(1);
    }
}
