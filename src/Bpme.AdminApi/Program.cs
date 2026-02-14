using Bpme.AdminApi;
using Bpme.AdminApi.Backgrounds;
using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Infrastructure.Config;
using Bpme.Infrastructure.DependencyInjection;
using Bpme.Infrastructure.Steps;
using Microsoft.AspNetCore.Http.Features;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
Console.Title = "bpme-admin-api";

builder.Host.UseSerilog((ctx, _, config) =>
{
    var logDir = ctx.Configuration["Logging:Directory"] ?? "Logs";
    var defaultName = ctx.Configuration["Logging:FilePath"] ?? "bpme.log";
    var baseDir = ctx.HostingEnvironment.ContentRootPath;
    var resolvedDir = Path.IsPathRooted(logDir) ? logDir : Path.Combine(baseDir, logDir);
    Directory.CreateDirectory(resolvedDir);

    config
        .MinimumLevel.Information()
        .Enrich.WithProperty("Process", "app")
        .Enrich.WithProperty("Step", "system")
        .Enrich.WithProperty("Iteration", "0")
        .MinimumLevel.Override("Microsoft.AspNetCore.Mvc", Serilog.Events.LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.AspNetCore.Mvc.Infrastructure", Serilog.Events.LogEventLevel.Warning)
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
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHttpClient();

var settings = builder.Configuration.Get<PipelineSettings>()
              ?? throw new InvalidOperationException("Не удалось загрузить настройки");

builder.Services.Configure<FormOptions>(o => { o.MultipartBodyLengthLimit = settings.AdminApi.MaxUploadBytes; });
builder.WebHost.ConfigureKestrel(o => { o.Limits.MaxRequestBodySize = settings.AdminApi.MaxUploadBytes; });

builder.Services.AddSingleton(settings);
builder.Services.AddSingleton<IPipelineDefinitionProvider, JsonPipelineDefinitionProvider>();
builder.Services.AddSingleton<IPipelineDefinitionRegistry, PipelineDefinitionRegistry>();
builder.Services.AddSingleton<ProcessIterationStore>();
builder.Services.AddSingleton<IProcessTriggerService, ProcessTriggerService>();
builder.Services.AddSingleton<FtpIngressService>();
builder.Services.AddBpmeInfrastructure(settings);

builder.Services.AddSingleton<IStepHandler, FtpScanHandler>();
builder.Services.AddSingleton<IStepHandler, ParseCsvHandler>();
builder.Services.AddSingleton<IStepHandler, LogSinkHandler>();
builder.Services.AddSingleton<IStepHandler, LogMessageHandler>();
builder.Services.AddSingleton<PipelineOrchestrator>();
builder.Services.AddHostedService<PipelineWorker>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.Use((context, next) =>
{
    var feature = context.Features.Get<IHttpMaxRequestBodySizeFeature>();
    if (feature != null && !feature.IsReadOnly)
    {
        feature.MaxRequestBodySize = settings.AdminApi.MaxUploadBytes;
    }

    return next();
});

app.Use(async (context, next) =>
{
    var logger = context.RequestServices.GetRequiredService<ILogger<Program>>();
    logger.LogInformation("HTTP {Method} {Path} start", context.Request.Method, context.Request.Path);
    try
    {
        await next();
        logger.LogInformation("HTTP {Method} {Path} done status={StatusCode}", context.Request.Method, context.Request.Path, context.Response.StatusCode);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "HTTP {Method} {Path} failed", context.Request.Method, context.Request.Path);
        throw;
    }
});

app.MapAdminApi(settings);

app.Run();

