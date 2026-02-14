using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;

namespace Bpme.AdminApi.Backgrounds;

/// <summary>
/// Фоновый воркер: регистрирует шаги и запускает процессы по расписанию.
/// </summary>
public sealed class PipelineWorker : BackgroundService
{
    private readonly ILogger<PipelineWorker> _logger;
    private readonly PipelineOrchestrator _orchestrator;
    private readonly IProcessTriggerService _triggerService;
    private readonly PipelineSettings _settings;
    private readonly IPipelineDefinitionRegistry _registry;

    public PipelineWorker(
        ILogger<PipelineWorker> logger,
        PipelineOrchestrator orchestrator,
        IProcessTriggerService triggerService,
        PipelineSettings settings,
        IPipelineDefinitionRegistry registry)
    {
        _logger = logger;
        _orchestrator = orchestrator;
        _triggerService = triggerService;
        _settings = settings;
        _registry = registry;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline worker started.");
        _orchestrator.RegisterHandlers();
        _logger.LogInformation("TriggerMode={Mode}", _settings.FtpDetection.TriggerMode);

        if (_settings.FtpDetection.TriggerMode == TriggerMode.Manual)
        {
            _logger.LogInformation("Background trigger disabled by TriggerMode=Manual.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        var defaultPeriodSeconds = _settings.FtpDetection.PeriodInSeconds;
        if (defaultPeriodSeconds <= 0)
        {
            _logger.LogWarning("Background scheduling disabled. PeriodInSeconds={Seconds}", defaultPeriodSeconds);
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        var scheduledDefinitions = _registry.GetAll()
            .Where(definition =>
            {
                var first = _registry.GetFirstStep(definition);
                return string.Equals(first.Name, "periodicallyTrigger", StringComparison.OrdinalIgnoreCase);
            })
            .ToList();

        if (scheduledDefinitions.Count == 0)
        {
            _logger.LogWarning("Background scheduling skipped. No periodicallyTrigger start step found.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
            return;
        }

        _logger.LogInformation("Background scheduling enabled. DefaultPeriod={Seconds}s", defaultPeriodSeconds);
        var schedules = new Dictionary<string, (int Period, DateTimeOffset NextRun, int MaxRuns, int Runs)>();
        foreach (var definition in scheduledDefinitions)
        {
            var triggerStep = _registry.GetFirstStep(definition);
            var period = defaultPeriodSeconds;
            var periodParam = triggerStep.GetParameter("periodInSeconds");
            if (!string.IsNullOrWhiteSpace(periodParam) && int.TryParse(periodParam, out var parsedPeriod))
            {
                period = parsedPeriod;
            }

            var maxRuns = 0;
            var maxRunsParam = triggerStep.GetParameter("maxRuns");
            if (!string.IsNullOrWhiteSpace(maxRunsParam) && int.TryParse(maxRunsParam, out var parsedMaxRuns))
            {
                maxRuns = parsedMaxRuns;
            }

            schedules[definition.Tag] = (period, DateTimeOffset.UtcNow, maxRuns, 0);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var definition in scheduledDefinitions)
            {
                var schedule = schedules[definition.Tag];
                if (now < schedule.NextRun)
                {
                    continue;
                }

                if (schedule.MaxRuns > 0 && schedule.Runs >= schedule.MaxRuns)
                {
                    continue;
                }

                await _triggerService.PublishTriggerAsync(definition, "periodicTrigger", ct: stoppingToken);
                using var scope = _logger.BeginScope(new Dictionary<string, object>
                {
                    ["Process"] = definition.Tag,
                    ["Step"] = "periodicTrigger"
                });
                _logger.LogInformation("trigger period={Period}", schedule.Period);

                schedules[definition.Tag] = (
                    schedule.Period,
                    now.AddSeconds(schedule.Period),
                    schedule.MaxRuns,
                    schedule.Runs + 1);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}
