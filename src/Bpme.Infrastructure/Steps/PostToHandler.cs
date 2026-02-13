using System.Net.Http;
using System.Text;
using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;

/// <summary>
/// Шаг отправки JSON по HTTP.
/// </summary>
public sealed class PostToHandler : IStepHandler
{
    private const string StepName = "postTo";
    private readonly IObjectStorage _storage;
    private readonly IEventBus _bus;
    private readonly PipelineDefinition _definition;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<PostToHandler> _logger;

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public PostToHandler(
        IObjectStorage storage,
        IEventBus bus,
        IPipelineDefinitionProvider definitionProvider,
        IHttpClientFactory httpFactory,
        ILogger<PostToHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _definition = definitionProvider.GetDefinition();
        _httpFactory = httpFactory;
        _logger = logger;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => TopicTag.From(_definition.GetPreviousStep(StepName).TopicTag);

    /// <summary>
    /// Обработать событие и отправить JSON.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId });

        var step = _definition.GetStep(StepName);
        var destination = step.GetParameter("destination");
        if (string.IsNullOrWhiteSpace(destination))
        {
            _logger.LogWarning("PostTo skipped: destination not set.");
            return;
        }

        if (!evt.Payload.TryGetValue("parsedPath", out var parsedPath))
        {
            _logger.LogWarning("Missing parsedPath in event payload");
            return;
        }

        _logger.LogInformation("PostTo start. url={Url} s3={S3}", destination, parsedPath);

        await using var stream = await _storage.GetAsync(parsedPath, ct);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var json = await reader.ReadToEndAsync();

        using var client = _httpFactory.CreateClient();
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await client.PostAsync(destination, content, ct);
        var body = await response.Content.ReadAsStringAsync(ct);

        _logger.LogInformation("PostTo done. status={StatusCode} bytes={Bytes}", (int)response.StatusCode, body?.Length ?? 0);

        var payload = new Dictionary<string, string>
        {
            ["parsedPath"] = parsedPath,
            ["status"] = ((int)response.StatusCode).ToString()
        };

        await _bus.PublishAsync(new PipelineEvent(TopicTag.From(step.TopicTag), evt.CorrelationId, payload), ct);
    }
}
