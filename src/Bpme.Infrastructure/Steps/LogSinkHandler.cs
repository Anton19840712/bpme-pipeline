using System.Text.Json;
using System.Text.Encodings.Web;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;

/// <summary>
/// Финальный шаг: логирование JSON из S3.
/// </summary>
public sealed class LogSinkHandler : IStepHandler, IOptionalStepHandler
{
    private readonly IObjectStorage _storage;
    private readonly ILogger<LogSinkHandler> _logger;
    private readonly PipelineTopicsSettings _topics;
    private readonly bool _enabled;

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public LogSinkHandler(IObjectStorage storage, PipelineSettings settings, ILogger<LogSinkHandler> logger)
    {
        _storage = storage;
        _enabled = settings.Sink.EnableLogStep;
        _logger = logger;
        _topics = settings.PipelineTopics;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => TopicTag.From(_topics.ParsingTopic);

    /// <summary>
    /// Признак включения обработчика.
    /// </summary>
    public bool Enabled => _enabled;

    /// <summary>
    /// Обработать событие парсинга.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId });

        if (!evt.Payload.TryGetValue("parsedPath", out var parsedPath))
        {
            _logger.LogWarning("Missing parsedPath in event payload");
            return;
        }

        var isDuplicate = evt.Payload.TryGetValue("isDuplicate", out var dup) && dup == "true";
        if (isDuplicate)
        {
            _logger.LogInformation("Duplicate file processing (consumer).");
        }

        _logger.LogInformation("Log sink start. s3={S3}", parsedPath);

        await using var stream = await _storage.GetAsync(parsedPath, ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var pretty = JsonSerializer.Serialize(doc, new JsonSerializerOptions
        {
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        });
        _logger.LogInformation("Parsed JSON:\n{Json}", pretty);
    }
}


