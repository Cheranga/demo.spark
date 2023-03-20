﻿using AutoFixture;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Demo.Spark.ETL.Extensions;
using Demo.Spark.ETL.Features.GetItemsInLocations;
using Demo.Spark.ETL.Features.Schemas;
using Demo.Spark.Tests.Models;
using Microsoft.Spark.Sql;

namespace Demo.Spark.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class EtlRunner : IDisposable
{
    private readonly DataFrame _itemLocations;
    private readonly DataFrame _items;
    private readonly SparkSession _spark;

    public EtlRunner()
    {
        _spark = SparkSession.Builder().AppName(nameof(EtlRunner)).GetOrCreate();
        var fixture = new Fixture();

        _items = _spark.GetDataFrame(fixture.CreateMany<Item>(1000));
        _itemLocations = _spark.GetDataFrame(fixture.CreateMany<ItemLocation>(1000));
    }

    public void Dispose() => _spark.Dispose();
    
    [Benchmark(Baseline = true)]
    public void ObjectOrientedTypedBuilder()
    {
        var _ = ItemLocationBuilder.Build(_items, _itemLocations);
    }

    [Benchmark]
    public void LooselyTypedBuilder()
    {
        var _ = LooselyTypedItemLocationBuilder.Build(_items, _itemLocations);
    }

    [Benchmark]
    public void StronglyTypedBuilder()
    {
        var _ = StronglyTypedItemLocationBuilder.Build(
            _items,
            _itemLocations,
            new ItemSchema(),
            new ItemLocationSchema()
        );
    }
}
