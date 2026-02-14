using System.Text.Encodings.Web;
using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;

/// <summary>
/// Финальный шаг: логирование JSON из S3.
/// </summary>
public sealed class LogSinkHandler : IStepHandler, IMultiTopicStepHandler, IStepNameProvider
{
    private readonly IObjectStorage _storage;
    private readonly PipelineSettings _settings;
    private readonly ILogger<LogSinkHandler> _logger;
    private readonly IPipelineDefinitionRegistry _registry;
    private const string StepNameConst = "log";

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public LogSinkHandler(
        IObjectStorage storage,
        PipelineSettings settings,
        IPipelineDefinitionRegistry registry,
        ILogger<LogSinkHandler> logger)
    {
        _storage = storage;
        _settings = settings;
        _logger = logger;
        _registry = registry;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => Topics.Count > 0 ? Topics[0] : TopicTag.From("unknown.parsing");

    /// <summary>
    /// Темы входа для всех включенных пайплайнов.
    /// </summary>
    public IReadOnlyList<TopicTag> Topics => ResolveTopics();

    /// <summary>
    /// Имя шага.
    /// </summary>
    public string StepName => StepNameConst;

    /// <summary>
    /// Обработать событие парсинга.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        if (!evt.Payload.TryGetValue("parsedPath", out var parsedPath))
        {
            _logger.LogWarning("Missing parsedPath in event payload");
            return;
        }

        var pipelineTag = evt.Payload.TryGetValue("pipelineTag", out var tag) ? tag : null;
        var iteration = evt.Payload.TryGetValue("iteration", out var iter) ? iter : "-";
        var definition = _registry.ResolveByInputTopic(StepNameConst, evt.Topic.Value, pipelineTag);
        _registry.GetStepByInputTopic(definition, StepNameConst, evt.Topic.Value);

        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["correlationId"] = evt.CorrelationId,
            ["Process"] = definition.Tag,
            ["Step"] = StepNameConst,
            ["Iteration"] = iteration
        });

        _logger.LogInformation("статус=started");

        var isDuplicate = evt.Payload.TryGetValue("isDuplicate", out var dup) && dup == "true";
        if (isDuplicate)
        {
            _logger.LogInformation("Дубликат файла: обработка в консумере.");
        }

        _logger.LogInformation("Log sink start. s3={S3} tag={Tag}", parsedPath, definition.Tag);

        await using var stream = await _storage.GetAsync(parsedPath, ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var items = doc.RootElement.ValueKind == JsonValueKind.Array ? doc.RootElement.GetArrayLength() : 0;
        if (_settings.Sink.LogJson)
        {
            var pretty = JsonSerializer.Serialize(doc, new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });
            _logger.LogInformation("Parsed JSON:\n{Json}", pretty);
        }
        else
        {
            _logger.LogInformation("Parsed JSON loaded. items={Count}", items);
        }

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






