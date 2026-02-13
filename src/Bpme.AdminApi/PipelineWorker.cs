using System.Collections.Generic;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bpme.AdminApi;

/// <summary>
/// Фоновый воркер для регистрации шагов и периодического триггера FTP.
/// </summary>
public sealed class PipelineWorker : BackgroundService
{
    private readonly ILogger<PipelineWorker> _logger;
    private readonly PipelineOrchestrator _orchestrator;
    private readonly IEventBus _eventBus;
    private readonly PipelineSettings _settings;

    /// <summary>
    /// Создать воркер конвейера.
    /// </summary>
    public PipelineWorker(
        ILogger<PipelineWorker> logger,
        PipelineOrchestrator orchestrator,
        IEventBus eventBus,
        PipelineSettings settings)
    {
        _logger = logger;
        _orchestrator = orchestrator;
        _eventBus = eventBus;
        _settings = settings;
    }

    /// <summary>
    /// Запуск фонового цикла.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline worker started.");
        _orchestrator.RegisterHandlers();

        var periodSeconds = _settings.FtpDetection.PeriodInSeconds;
        if (periodSeconds > 0)
        {
            _logger.LogInformation("FTP polling enabled. Period={Seconds}s", periodSeconds);
            while (!stoppingToken.IsCancellationRequested)
            {
                var correlationId = Guid.NewGuid().ToString("N");
                using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = correlationId });
                var evt = new PipelineEvent(TopicTag.From(_settings.PipelineTopics.TriggerTopic), correlationId, new Dictionary<string, string>());
                await _eventBus.PublishAsync(evt);
                await Task.Delay(TimeSpan.FromSeconds(periodSeconds), stoppingToken);
            }
        }
        else
        {
            _logger.LogWarning("FTP polling disabled. PeriodInSeconds={Seconds}", periodSeconds);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
