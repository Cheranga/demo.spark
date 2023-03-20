namespace Demo.Spark.ETL.Core;

public class Error
{
    public int Code { get; }
    public string Message { get; }
    public Exception Exception { get; }

    private Error(int code, string message, Exception exception)
    {
        Code = code;
        Message = message;
        Exception = exception;
    }

    public static Error New(int code, string message, Exception exception) => new(code, message, exception);
    public static Error New(int code, string message) => new(code, message, new Exception(message));
}