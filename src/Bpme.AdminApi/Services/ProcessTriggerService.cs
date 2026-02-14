using Bpme.Application.Pipeline;
using Bpme.Application.Abstractions;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;

namespace Bpme.AdminApi;

public sealed class ProcessTriggerService : IProcessTriggerService
{
    private readonly IEventBus _eventBus;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly ProcessIterationStore _iterations;
    private readonly ILogger<ProcessTriggerService> _logger;

    public ProcessTriggerService(
        IEventBus eventBus,
        IPipelineDefinitionRegistry registry,
        ProcessIterationStore iterations,
        ILogger<ProcessTriggerService> logger)
    {
        _eventBus = eventBus;
        _registry = registry;
        _iterations = iterations;
        _logger = logger;
    }

    public async Task PublishTriggerAsync(
        PipelineDefinition definition,
        string triggerStepName,
        IReadOnlyDictionary<string, string>? payload = null,
        CancellationToken ct = default)
    {
        var iteration = _iterations.Next(definition.Tag);
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["Process"] = definition.Tag,
            ["Step"] = triggerStepName,
            ["Iteration"] = iteration.ToString()
        });

        if (iteration > 1)
        {
            _logger.LogInformation(" ");
        }

        var topic = _registry.GetFirstStep(definition).TopicTag;
        var eventPayload = payload?.ToDictionary(x => x.Key, x => x.Value)
            ?? new Dictionary<string, string>();
        eventPayload["pipelineTag"] = definition.Tag;
        eventPayload["iteration"] = iteration.ToString();

        var evt = new PipelineEvent(TopicTag.From(topic), Guid.NewGuid().ToString("N"), eventPayload);
        await _eventBus.PublishAsync(evt, ct);

        _logger.LogInformation("процесс начался");
        _logger.LogInformation("trigger published. topic={Topic}", topic);
    }
}
