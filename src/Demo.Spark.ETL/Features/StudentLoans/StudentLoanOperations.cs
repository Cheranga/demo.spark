using Demo.Spark.ETL.Core;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.Schemas;

namespace Demo.Spark.ETL.Features.StudentLoans;

public static class StudentLoanOperations
{
    public static Box<StudentLoansDataFrame> GetStudentLoans(
        StudentsDataFrame students,
        LoansDataFrame loans,
        int studentId,
        string loanType
    ) =>
        from student in FindStudent(students, studentId)
            .MapFail(Error.New("error when getting student"))
        from loan in FindLoans(loans, loanType).MapFail(Error.New("error when getting loan"))
        from studentLoans in GetLoansForStudent(student, loan)
            .MapFail(Error.New("error when getting student loans"))
        select studentLoans;

    public static Box<StudentLoansDataFrame> GetStudentLoans(
        StudentsDataFrame students,
        LoansDataFrame loans,
        int studentId,
        int loanId
    ) =>
        from student in FindStudent(students, studentId)
        from loan in FindLoans(loans, loanId)
        from studentLoans in GetLoansForStudent(student, loan)
        select studentLoans;

    private static Box<StudentsDataFrame> FindStudent(StudentsDataFrame students, int studentId) =>
        students.FindStudentById(studentId).ToBox();

    private static Box<LoansDataFrame> FindLoans(LoansDataFrame loans, string loanType) =>
        loans.GetLoan(loanType).ToBox();

    private static Box<LoansDataFrame> FindLoans(LoansDataFrame loans, int loanId) =>
        loans.GetLoan(loanId).ToBox();

    private static Box<StudentLoansDataFrame> GetLoansForStudent(
        StudentsDataFrame students,
        LoansDataFrame loans
    ) =>
        new StudentLoansDataFrame(
            students
                .Join<LoansDataFrame, LoanSchema>(
                    loans,
                    students
                        .Col(x => x.Id)
                        .EqualTo(loans.Col(x => x.StudentId))
                        .And(students.Col(x => x.LoanId).EqualTo(loans.Col(x => x.Id)))
                )
                .Select(
                    loans.ColAs(x => x.StudentId, nameof(StudentLoanSchema.CustomerId)),
                    students.ColAs(x => x.Name, nameof(StudentLoanSchema.CustomerFullName)),
                    loans.ColAs(x => x.Name, nameof(StudentLoanSchema.BankLoanType)),
                    loans.ColAs(x => x.StartDate, nameof(StudentLoanSchema.LoanStartDate)),
                    loans.ColAs(x => x.EndDate, nameof(StudentLoanSchema.LoanEndDate)),
                    loans.ColAs(x => x.IsActive, nameof(StudentLoanSchema.IsDue))
                )
        ).ToBox();
}
