using Demo.Spark.ETL.Features.StudentLoans;

namespace Demo.Spark.Tests.Features.StudentLoans;

[Collection(SparkTestCollection.Name)]
public class StudentLoanServiceTests
{
    private readonly LoansDataFrame _loans;
    private readonly StudentsDataFrame _students;

    public StudentLoanServiceTests(SparkInitializer initializer)
    {
        var studentSchema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType()),
                new StructField("LoanId", new IntegerType())
            }
        );

        var loanSchema = new StructType(
            new[]
            {
                new StructField("Id", new IntegerType()),
                new StructField("Name", new StringType()),
                new StructField("StudentId", new IntegerType())
            }
        );

        _students = new StudentsDataFrame(
            initializer.Spark.CreateDataFrame(
                new GenericRow[]
                {
                    new(new object[] { 1, "Cheranga", 100 }),
                    new(new object[] { 1, "Cheranga", 200 })
                },
                studentSchema
            )
        );

        _loans = new LoansDataFrame(
            initializer.Spark.CreateDataFrame(
                new GenericRow[]
                {
                    new(new object[] { 100, "Long Term Loan", 1 }),
                    new(new object[] { 200, "Short Term Loan", 1 })
                },
                loanSchema
            )
        );
    }

    [Fact]
    public void Test()
    {
        var studentLoanService = new StudentLoanService(_students, _loans);
        var studentLongTermLoans = studentLoanService.GetStudentLoans(1, "Long Term Loan");
        var studentShortTermLoans = studentLoanService.GetStudentLoans(1, "Short Term Loan");
    }
}
