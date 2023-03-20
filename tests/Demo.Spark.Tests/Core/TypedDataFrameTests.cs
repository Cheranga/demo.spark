﻿using FluentAssertions;

namespace Demo.Spark.Tests.Core;

[Collection(SparkTestCollection.Name)]
public class TypedDataFrameTests
{
    private readonly StudentsDataFrame _dataFrame;

    public TypedDataFrameTests(SparkInitializer initializer)
    {
        var schema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType())
            }
        );
        _dataFrame = new StudentsDataFrame(
            initializer.Spark.CreateDataFrame(
                new GenericRow[] { new(new object[] { 1, "A" }), new(new object[] { 2, "B" }) },
                schema
            )
        );
    }

    [Fact]
    public void FindStudentById()
    {
        var studentWithId = _dataFrame.FindStudentById(1);
        var student = studentWithId.ToDataFrame().Collect().First();
        student.GetAs<string>("Name").Should().Be("A");
    }

    [Fact]
    public void FindStudentByName()
    {
        var studentWithName = _dataFrame.FindStudentByName("B");
        var student = studentWithName.ToDataFrame().Collect().First();
        student.GetAs<int>("Id").Should().Be(2);
    }
    
    [Fact]
    public void FilterStudentById()
    {
        var studentWithId = _dataFrame.Filter(x => x.Id, 1);
        var student = studentWithId.ToDataFrame().Collect().First();
        student.GetAs<string>("Name").Should().Be("A");
    }

    [Fact]
    public void FilterStudentByName()
    {
        var studentWithName = _dataFrame.Filter(x => x.Name, "A");
        var student = studentWithName.ToDataFrame().Collect().First();
        student.GetAs<int>("Id").Should().Be(1);
    }
}
