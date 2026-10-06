using System.Collections.Frozen;
using System.Text.Json;

namespace DotnetJobKit.Handlers;

public sealed class JobHandlerRegistry
{
    private readonly FrozenDictionary<string, JobContractRegistration> _byContract;
    private readonly FrozenDictionary<Type, JobContractRegistration> _byJobType;
    private readonly JsonSerializerOptions _jsonOptions;

    public JobHandlerRegistry(IEnumerable<JobContractRegistration> registrations, JsonSerializerOptions? jsonOptions = null)
    {
        var list = registrations.ToList();
        if (list.Count == 0)
            throw new InvalidOperationException("At least one job handler must be registered.");

        _byContract = list.ToFrozenDictionary(r => ContractKey(r.ContractName, r.ContractVersion));
        _byJobType = list.ToFrozenDictionary(r => r.JobType);
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public JobContractRegistration GetByJobType(Type jobType) =>
        _byJobType.TryGetValue(jobType, out var registration)
            ? registration
            : throw new InvalidOperationException($"Job type '{jobType.FullName}' is not registered.");

    public JobContractRegistration GetByContract(string contractName, int contractVersion) =>
        _byContract.TryGetValue(ContractKey(contractName, contractVersion), out var registration)
            ? registration
            : throw new InvalidOperationException($"Contract '{contractName}' v{contractVersion} is not registered.");

    public string Serialize<TJob>(TJob job)
        where TJob : notnull
    {
        var registration = GetByJobType(typeof(TJob));
        var json = JsonSerializer.Serialize(job, registration.JobType, _jsonOptions);
        return json;
    }

    public object Deserialize(string contractName, int contractVersion, string payload)
    {
        var registration = GetByContract(contractName, contractVersion);
        return JsonSerializer.Deserialize(payload, registration.JobType, _jsonOptions)
            ?? throw new InvalidOperationException($"Payload for '{contractName}' deserialized to null.");
    }

    private static string ContractKey(string contractName, int contractVersion) =>
        $"{contractName}::{contractVersion}";
}
