namespace DotnetJobKit.Handlers;

public sealed class JobContractRegistration
{
    public required Type JobType { get; init; }
    public required Type HandlerType { get; init; }
    public required string ContractName { get; init; }
    public required int ContractVersion { get; init; }
    public required string DefaultQueue { get; init; }

    public JobHandleInvoker? Invoker { get; init; }
}
