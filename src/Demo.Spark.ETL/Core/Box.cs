using System.Diagnostics.CodeAnalysis;

namespace Demo.Spark.ETL.Core;

[ExcludeFromCodeCoverage]
public class Box<T>
{
    private readonly T _data = default!;
    public bool IsEmpty { get; init; }

    public Box(T data)
    {
        _data = data;
        IsEmpty = false;
    }

    public Box()
    {
        IsEmpty = true;
    }

    public T Data => _data;
}