using System.Diagnostics.CodeAnalysis;

namespace Demo.Spark.ETL.Core;

[ExcludeFromCodeCoverage]
public class Box<T>
{
    private readonly T _data = default!;
    public bool IsEmpty { get; init; }
    public Error Error { get; }

    public bool IsError => Error != null && Error.Code != 0;

    private Box(T data)
    {
        _data = data;
        IsEmpty = false;
    }

    private Box()
    {
        IsEmpty = true;
    }

    private Box(Error error)
    {
        Error = error;
    }

    public T Data => _data;

    public static Box<T> New(T data) => new(data);
    public static Box<T> New() => new();
    public static Box<T> Fail(Error error) => new(error);
}