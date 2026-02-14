using Bpme.Application.Pipeline;

namespace Bpme.AdminApi;

public interface IProcessTriggerService
{
    Task PublishTriggerAsync(
        PipelineDefinition definition,
        string triggerStepName,
        IReadOnlyDictionary<string, string>? payload = null,
        CancellationToken ct = default);
}
