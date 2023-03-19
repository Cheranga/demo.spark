// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Running;
using Demo.Spark.Benchmarks;

Console.WriteLine("Hello, World!");

BenchmarkRunner.Run<EtlRunner>();
