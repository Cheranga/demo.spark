# Demo.Spark

## Debugging the application

Run the below command, to debug the application.

```
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local "C:\Cheranga\Work\Personal\Learn\Spark\Demo.Spark\tests\Demo.Spark.Tests\bin\Debug\net6.0\microsoft-spark-3-1_2.12-2.1.1.jar" debug
```

## Using Expression Trees

Getting the name using an expression tree

```csharp
public static string Col<TSchema>(this Expression<Func<TSchema, DataType>> exp)
    where TSchema : ISchema
{
    if (exp.Body is not MemberExpression memberExp)
        memberExp = ((exp.Body as UnaryExpression)!.Operand as MemberExpression)!;
    return memberExp.Member.Name;
}
```