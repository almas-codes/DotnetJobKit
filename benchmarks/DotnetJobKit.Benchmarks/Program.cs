using BenchmarkDotNet.Running;
using DotnetJobKit.Benchmarks;

if (args.Contains("--quick"))
{
    BenchmarkRunner.Run<SqliteClaimBenchmarks>();
}
else
{
    BenchmarkRunner.Run(typeof(Program).Assembly);
}
