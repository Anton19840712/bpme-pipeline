using Bpme.Application.Abstractions;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;
public sealed class LogMessageHandler : IStepHandler, IMultiTopicStepHandler, IStepNameProvider
{
    private const string StepNameConst = "logMessage";
    private readonly IEventBus _bus;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly ILogger<LogMessageHandler> _logger;
    public LogMessageHandler(IEventBus bus, IPipelineDefinitionRegistry registry, ILogger<LogMessageHandler> logger)
    {
        _bus = bus;
        _registry = registry;
        _logger = logger;
    }
    public TopicTag Topic => Topics.Count > 0 ? Topics[0] : TopicTag.From("unknown.input");
    public IReadOnlyList<TopicTag> Topics => ResolveTopics();
    public string StepName => StepNameConst;
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        var pipelineTag = evt.Payload.TryGetValue("pipelineTag", out var tag) ? tag : null;
        var iteration = evt.Payload.TryGetValue("iteration", out var iter) ? iter : "-";
        var definition = _registry.ResolveByInputTopic(StepNameConst, evt.Topic.Value, pipelineTag);
        var step = _registry.GetStepByInputTopic(definition, StepNameConst, evt.Topic.Value);

        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["correlationId"] = evt.CorrelationId,
            ["Process"] = definition.Tag,
            ["Step"] = StepNameConst,
            ["Iteration"] = iteration
        });

        _logger.LogInformation("статус=started");

        var message = step.GetParameter("message") ?? "logMessage step";
        var createdFile = evt.Payload.TryGetValue("createdFile", out var filePath) ? filePath : null;
        if (!string.IsNullOrWhiteSpace(createdFile))
        {
            _logger.LogInformation("LogMessage: {Message} createdFile={File}", message, createdFile);
        }
        else
        {
            _logger.LogInformation("LogMessage: {Message}", message);
        }

        var payload = new Dictionary<string, string>(evt.Payload)
        {
            ["Process"] = definition.Tag
        };

        await _bus.PublishAsync(new PipelineEvent(TopicTag.From(step.TopicTag), evt.CorrelationId, payload), ct);

        _logger.LogInformation("статус=finished");

        if (_registry.IsLastStepByInputTopic(definition, StepNameConst, evt.Topic.Value))
        {
            _logger.LogInformation("процесс завершён");
        }
    }

    private IReadOnlyList<TopicTag> ResolveTopics()
    {
        var topics = new List<TopicTag>();
        foreach (var definition in _registry.GetAll())
        {
            var inputTopics = _registry.GetInputTopics(definition, StepNameConst);
            foreach (var input in inputTopics)
            {
                topics.Add(TopicTag.From(input));
            }
        }

        return topics;
    }
}






