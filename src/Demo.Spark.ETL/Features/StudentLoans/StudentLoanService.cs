using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;

namespace Demo.Spark.ETL.Features.StudentLoans;

public class StudentLoanService
{
    private readonly LoansDataFrame _loans;
    private readonly StudentsDataFrame _students;

    public StudentLoanService(StudentsDataFrame students, LoansDataFrame loans)
    {
        _students = students;
        _loans = loans;
    }

    public Box<StudentLoansDataFrame> GetStudentLoans(int studentId, string loanType) =>
        from student in _students.FindStudentById(studentId).ToBox()
        from loan in _loans.GetLoan(loanType).ToBox()
        from studentLoans in GetLoansForStudent(student, loan)
        select studentLoans;

    private static Box<StudentLoansDataFrame> GetLoansForStudent(
        StudentsDataFrame students,
        LoansDataFrame loans
    ) =>
        new StudentLoansDataFrame(
            students
                .JoinWith<LoansDataFrame, LoanSchema>(
                    loans,
                    students
                        .Col(x => x.Id)
                        .EqualTo(loans.Col(x => x.StudentId))
                        .And(students.Col(x => x.LoanId).EqualTo(loans.Col(x => x.Id)))
                )
                .Select(
                    loans.ColAs(x => x.StudentId, nameof(StudentLoanSchema.CustomerId)),
                    students.ColAs(x => x.Name, nameof(StudentLoanSchema.CustomerFullName)),
                    loans.ColAs(x => x.Name, nameof(StudentLoanSchema.BankLoanType))
                )
        ).ToBox();
}
