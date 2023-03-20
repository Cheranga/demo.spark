using System.Diagnostics.CodeAnalysis;

namespace Demo.Spark.ETL.Core;

[ExcludeFromCodeCoverage]
public class Box<T>
{
    private readonly T _data = default!;
    public bool IsEmpty { get; init; }

    private Box(T data)
    {
        _data = data;
        IsEmpty = false;
    }

    private Box()
    {
        IsEmpty = true;
    }

    public T Data => _data;

    public static Box<T> New(T data) => new(data);
    public static Box<T> New() => new();
}