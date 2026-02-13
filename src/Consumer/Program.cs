using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Logging;
using Bpme.Domain.Abstractions;
using Bpme.Infrastructure.Bus;
using Bpme.Infrastructure.Config;
using Bpme.Infrastructure.Storage;
using Bpme.Infrastructure.Steps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bpme.ConsumerApp;

/// <summary>
/// Точка входа консюмера.
/// </summary>
public sealed class Program
{
    /// <summary>
    /// Главный метод.
    /// </summary>
    public static async Task Main(string[] args)
    {
        Console.Title = "bpme-consumer";
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .Build();
        var logPath = config["Logging:FilePath"] ?? "bpme.log";
        if (!Path.IsPathRooted(logPath))
        {
            logPath = Path.Combine(Directory.GetCurrentDirectory(), logPath);
        }

        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.SetBasePath(AppContext.BaseDirectory);
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.AddProvider(new FileLoggerProvider(logPath, LogLevel.Information));
            })
            .ConfigureServices(services =>
            {
                services.AddSingleton<ISettingsProvider, AppSettingsProvider>();
                services.AddSingleton(sp => sp.GetRequiredService<ISettingsProvider>().Load());

                services.AddSingleton<IEventBus>(sp =>
                {
                    var settings = sp.GetRequiredService<PipelineSettings>();
                    var logger = sp.GetRequiredService<ILogger<RabbitMqEventBus>>();
                    return new RabbitMqEventBus(
                        settings.RabbitMq.Host,
                        settings.RabbitMq.Port,
                        settings.RabbitMq.User,
                        settings.RabbitMq.Password,
                        settings.RabbitMq.VirtualHost,
                        settings.RabbitMq.Exchange,
                        settings.RabbitMq.QueuePrefix,
                        logger);
                });

                services.AddSingleton<IObjectStorage>(sp =>
                {
                    var s3 = sp.GetRequiredService<PipelineSettings>().S3;
                    var logger = sp.GetRequiredService<ILogger<S3ObjectStorage>>();
                    return new S3ObjectStorage(s3.Endpoint, s3.AccessKey, s3.SecretKey, s3.Bucket, s3.UseSsl, logger);
                });

                services.AddSingleton<IStepHandler, LogSinkHandler>();

                services.AddSingleton<PipelineOrchestrator>();
            })
            .Build();

        var logger = host.Services.GetRequiredService<ILogger<Program>>();
        var orchestrator = host.Services.GetRequiredService<PipelineOrchestrator>();
        var settings = host.Services.GetRequiredService<PipelineSettings>();

        logger.LogInformation("Consumer started. Log file path: {Path}", logPath);

        if (settings.Control != null && !string.IsNullOrWhiteSpace(settings.Control.PauseFilePath))
        {
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    if (File.Exists(settings.Control.PauseFilePath))
                    {
                        logger.LogWarning("Pause file detected. Consumer will остановиться. Path={Path}", settings.Control.PauseFilePath);
                        Environment.Exit(0);
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            });
        }

        orchestrator.RegisterHandlers();

        await Task.Delay(Timeout.Infinite);
    }
}
