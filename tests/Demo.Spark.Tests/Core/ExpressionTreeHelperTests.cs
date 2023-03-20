using System.Linq.Expressions;
using Demo.Spark.ETL.Core;
using Demo.Spark.Tests.Models;

namespace Demo.Spark.Tests.Core;

[Collection(SparkTestCollection.Name)]
public class ExpressionTreeHelperTests
{
    private readonly SparkSession _spark;

    public ExpressionTreeHelperTests(SparkInitializer initializer)
    {
        _spark = initializer.Spark;
    }
    
    [Fact]
    public void Test()
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
        
        // Expression<Func<StudentsDataFrame, bool>> exp = x => dataFrame.Where(x.Col(_ => _.Id).EqualTo(lit))
        // var unaryExp = (exp.Body as UnaryExpression);
        // var memberExp = unaryExp.Operand as MemberExpression;


        Expression<Func<Student, bool>> exp = x => x.StudentId > 1;
        
        // StudentsDataFrame GetStudents(StudentsDataFrame students, Expression<Func<Student, bool>> filter)
        // {
        //     
        // }

    }
    
    
}