using Demo.Spark.ETL.Features.Schemas;
using Demo.Spark.ETL.Features.StudentLoans;
using FluentAssertions;
using static Demo.Spark.ETL.Extensions.SparkExtensions;
namespace Demo.Spark.Tests.Features.StudentLoans;

[Collection(SparkTestCollection.Name)]
public class StudentLoanOperationsTests
{
    private readonly LoansDataFrame _loans;
    private readonly StudentsDataFrame _students;

    public StudentLoanOperationsTests(SparkInitializer initializer)
    {
        _students = new StudentsDataFrame(
            initializer.Spark.CreateDataFrame(
                new GenericRow[]
                {
                    new(new object[] { 1, "Cheranga", 100 }),
                    new(new object[] { 1, "Cheranga", 200 })
                },
                ToSchema<StudentSchema>()
            )
        );

        _loans = new LoansDataFrame(
            initializer.Spark.CreateDataFrame(
                new GenericRow[]
                {
                    new(new object[] { 100, "Long Term Loan", 1, new Timestamp(DateTime.UtcNow), new Timestamp(DateTime.UtcNow.AddYears(10)), true }),
                    new(new object[] { 200, "Short Term Loan", 1, new Timestamp(DateTime.UtcNow), new Timestamp(DateTime.UtcNow.AddYears(5)), true })
                },
                ToSchema<LoanSchema>()
            )
        );
    }

    [Theory]
    [InlineData(1, "Long Term Loan")]
    [InlineData(1, "long term loan")]
    [InlineData(1, "Short Term Loan")]
    [InlineData(1, "short term loan")]
    public void FindingStudentLoansWithLoanName(int studentId, string loanType)
    {
        var studentLoans = StudentLoanOperations.GetStudentLoans(
            _students,
            _loans,
            studentId,
            loanType
        );
        studentLoans.IsEmpty.Should().BeFalse();

        var records = studentLoans.Data.ToRaw().Collect().ToList();
        records.Should().NotBeEmpty();

        records.First().GetAs<int>(nameof(StudentLoanSchema.CustomerId)).Should().Be(studentId);
        records
            .First()
            .GetAs<string>(nameof(StudentLoanSchema.CustomerFullName))
            .Should()
            .Be("Cheranga");
        records
            .First()
            .GetAs<string>(nameof(StudentLoanSchema.BankLoanType))
            .ToUpper()
            .Should()
            .Be(loanType.ToUpper());
    }
    
    [Theory]
    [InlineData(1, 100, "long term loan")]
    [InlineData(1, 200, "short term loan")]
    public void FindingStudentLoansWithLoanId(int studentId, int loanId, string expectedLoanType)
    {
        var studentLoans = StudentLoanOperations.GetStudentLoans(
            _students,
            _loans,
            studentId,
            loanId
        );
        studentLoans.IsEmpty.Should().BeFalse();

        var records = studentLoans.Data.ToRaw().Collect().ToList();
        records.Should().NotBeEmpty();

        records.First().GetAs<int>(nameof(StudentLoanSchema.CustomerId)).Should().Be(studentId);
        records
            .First()
            .GetAs<string>(nameof(StudentLoanSchema.CustomerFullName))
            .Should()
            .Be("Cheranga");
        records
            .First()
            .GetAs<string>(nameof(StudentLoanSchema.BankLoanType))
            .ToUpper()
            .Should()
            .Be(expectedLoanType.ToUpper());
    }
}
