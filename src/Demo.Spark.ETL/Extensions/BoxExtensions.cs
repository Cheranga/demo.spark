using System.Diagnostics.CodeAnalysis;
using Demo.Spark.ETL.Core;

namespace Demo.Spark.ETL.Extensions;

[ExcludeFromCodeCoverage]
public static class BoxExtensions
{
    public static Box<T> ToBox<T>(this T data) => Box<T>.New(data);

    /// <summary>
    /// Validate, Extract, Transform and Lift (If Valid)
    /// </summary>
    public static Box<TB> Select<TA, TB>(this Box<TA> box, Func<TA, TB> map)
    {
        if (box.IsEmpty)
            return Box<TB>.New();

        var extracted = box.Data;

        TB transformedItem = map(extracted);

        return Box<TB>.New(transformedItem);
    }

    /// <summary>
    /// Validate, Extract, Transform, Project(Transform, Extract) and automatic Lift
    /// </summary>
    public static Box<TC> SelectMany<TA, TB, TC>(this Box<TA> box, Func<TA, Box<TB>> bind, Func<TA, TB, TC> project)
    {
        if(box.IsEmpty)
            return Box<TC>.New();

        var extract = box.Data;

        Box<TB> liftedResult = bind(extract);

        if(liftedResult.IsEmpty)
            return Box<TC>.New();

        TC t2 = project(extract, liftedResult.Data);
        return Box<TC>.New(t2);
    }

    /// <summary>
    /// Validate, Extract, Transform and Lift (If Valid)
    /// Check/Validate then transform to T and lift into Box<T>
    /// </summary>
    public static Box<TB> Bind<TA, TB>(this Box<TA> box, Func<TA, Box<TB>> bind)
    {
        if(box.IsEmpty)
            return Box<TB>.New();

        TA extract = box.Data;

        Box<TB> transformedAndLifted = bind(extract);

        return transformedAndLifted;
    }

    /// <summary>
    /// Validate, Extract, Transform and automatic Lift (If Valid) 
    /// </summary>
    public static Box<TB> Map<TA, TB>(this Box<TA> box, Func<TA, TB> select)
    {
        if(box.IsEmpty)
            return Box<TB>.New();

        TA extract = box.Data;

        TB transformed = select(extract);

        return Box<TB>.New(transformed);
    }
}