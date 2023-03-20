namespace Demo.Spark.Tests.Models;

public class Student
{
    public int StudentId { get; set; }
    public string StudentName { get; set; }

    public Student(int id, string name)
    {
        StudentId = id;
        StudentName = name;
    }
}