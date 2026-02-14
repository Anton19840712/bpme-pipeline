using Bpme.Application.Abstractions;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Application.Pipeline;
public sealed class PipelineOrchestrator
{
    private readonly IEventBus _eventBus;
    private readonly IEnumerable<IStepHandler> _handlers;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly ILogger<PipelineOrchestrator> _logger;
    public PipelineOrchestrator(
        IEventBus eventBus,
        IEnumerable<IStepHandler> handlers,
        IPipelineDefinitionRegistry registry,
        ILogger<PipelineOrchestrator> logger)
    {
        _eventBus = eventBus;
        _handlers = handlers;
        _registry = registry;
        _logger = logger;
    }
    public void RegisterHandlers()
    {
        foreach (var handler in _handlers)
        {
            if (handler is IMultiTopicStepHandler multi && multi.Topics.Count > 0)
            {
                foreach (var topic in multi.Topics)
                {
                    LogSubscription(handler, topic);
                    _eventBus.Subscribe(topic, handler.HandleAsync);
                }
            }
            else if (handler is IMultiTopicStepHandler)
            {
                continue;
            }
            else
            {
                LogSubscription(handler, handler.Topic);
                _eventBus.Subscribe(handler.Topic, handler.HandleAsync);
            }
        }
    }

    private void LogSubscription(IStepHandler handler, TopicTag topic)
    {
        if (handler is not IStepNameProvider named)
        {
            using (_logger.BeginScope(new Dictionary<string, object>
            {
                ["Process"] = "app",
                ["Step"] = "system"
            }))
            {
                _logger.LogInformation("subscribed topic={Topic}", topic.Value);
            }
            return;
        }

        try
        {
            var definition = _registry.ResolveByInputTopic(named.StepName, topic.Value);
            using (_logger.BeginScope(new Dictionary<string, object>
            {
                ["Process"] = definition.Tag,
                ["Step"] = named.StepName
            }))
            {
                _logger.LogInformation("subscribed topic={Topic}", topic.Value);
            }
        }
        catch
        {
            using (_logger.BeginScope(new Dictionary<string, object>
            {
                ["Process"] = "app",
                ["Step"] = named.StepName
            }))
            {
                _logger.LogInformation("subscribed topic={Topic}", topic.Value);
            }
        }
    }
}

