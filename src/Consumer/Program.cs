using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Infrastructure.Bus;
using Bpme.Infrastructure.Config;
using Bpme.Infrastructure.Storage;
using Bpme.Infrastructure.Steps;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace Consumer;

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
        Console.Title = "Consumer | bpme.consumer";

        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.SetBasePath(AppContext.BaseDirectory);
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
            })
            .UseSerilog((ctx, _, log) =>
            {
                var logDir = ctx.Configuration["Logging:Directory"] ?? "Logs";
                var defaultName = ctx.Configuration["Logging:FilePath"] ?? "bpme.log";
                var baseDir = ctx.HostingEnvironment.ContentRootPath;
                var resolvedDir = Path.IsPathRooted(logDir) ? logDir : Path.Combine(baseDir, logDir);
                Directory.CreateDirectory(resolvedDir);

                log.MinimumLevel.Information()
                    .Enrich.WithProperty("Process", "app")
                    .Enrich.WithProperty("Step", "system")
                    .Enrich.WithProperty("Iteration", "0")
                    .Enrich.FromLogContext()
                    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{Process}][{Iteration}][{Step}]: {Message:lj}{NewLine}{Exception}")
                    .WriteTo.Map(
                        "Process",
                        "app",
                        (tag, wt) =>
                        {
                            var fileName = string.Equals(tag, "app", StringComparison.OrdinalIgnoreCase)
                                ? defaultName
                                : $"{tag}.log";
                            var path = Path.Combine(resolvedDir, fileName);
                            wt.File(path, outputTemplate: "{Timestamp:O} [{Level}] {SourceContext} {Message:lj}{NewLine}{Exception}", shared: true);
                        });
            })
            .ConfigureServices(services =>
            {
                services.AddSingleton<ISettingsProvider, AppSettingsProvider>();
                services.AddSingleton(sp => sp.GetRequiredService<ISettingsProvider>().Load());
                services.AddSingleton<IPipelineDefinitionProvider, JsonPipelineDefinitionProvider>();
                services.AddSingleton<IPipelineDefinitionRegistry, PipelineDefinitionRegistry>();

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
        logger.LogInformation("Consumer started.");

        orchestrator.RegisterHandlers();

        await Task.Delay(Timeout.Infinite);
    }
}


