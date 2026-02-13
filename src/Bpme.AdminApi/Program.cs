using System.Net;
using System.Text;
using System.Text.Json;
using Bpme.AdminApi;
using Logging;
using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Bpme.Infrastructure.Bus;
using Bpme.Infrastructure.Config;
using Bpme.Infrastructure.Storage;
using Bpme.Infrastructure.Steps;
using FluentFTP;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Настройка логирования.
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
var logPath = builder.Configuration["Logging:FilePath"] ?? "bpme.log";
if (!Path.IsPathRooted(logPath))
{
    logPath = Path.Combine(Directory.GetCurrentDirectory(), logPath);
}
builder.Logging.AddProvider(new FileLoggerProvider(logPath, LogLevel.Information));

// Swagger.
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHttpClient();


// Настройки.
var settings = builder.Configuration.Get<PipelineSettings>()
              ?? throw new InvalidOperationException("Не удалось загрузить настройки");

// Ограничения для multipart (размер загрузки) берём из конфигурации.
builder.Services.Configure<FormOptions>(o => { o.MultipartBodyLengthLimit = settings.AdminApi.MaxUploadBytes; });
builder.WebHost.ConfigureKestrel(o => { o.Limits.MaxRequestBodySize = settings.AdminApi.MaxUploadBytes; });


builder.Services.AddSingleton(settings);
builder.Services.AddSingleton<IPipelineDefinitionProvider, JsonPipelineDefinitionProvider>();

// Инфраструктура.
builder.Services.AddSingleton<S3ObjectStorage>(sp =>
    new S3ObjectStorage(
        settings.S3.Endpoint,
        settings.S3.AccessKey,
        settings.S3.SecretKey,
        settings.S3.Bucket,
        settings.S3.UseSsl,
        sp.GetRequiredService<ILogger<S3ObjectStorage>>()
    ));

builder.Services.AddSingleton<IObjectStorage>(sp => sp.GetRequiredService<S3ObjectStorage>());

builder.Services.AddSingleton<IStateStore>(sp =>
{
    var state = settings.FtpDetection.StateStore;
    return new S3StateStore(
        settings.S3.Endpoint,
        settings.S3.AccessKey,
        settings.S3.SecretKey,
        state.Bucket,
        state.Prefix,
        settings.S3.UseSsl,
        sp.GetRequiredService<ILogger<S3StateStore>>());
});

builder.Services.AddSingleton<RabbitMqEventBus>(sp =>
    new RabbitMqEventBus(
        settings.RabbitMq.Host,
        settings.RabbitMq.Port,
        settings.RabbitMq.User,
        settings.RabbitMq.Password,
        settings.RabbitMq.VirtualHost,
        settings.RabbitMq.Exchange,
        settings.RabbitMq.QueuePrefix,
        sp.GetRequiredService<ILogger<RabbitMqEventBus>>()
    ));

builder.Services.AddSingleton<IEventBus>(sp => sp.GetRequiredService<RabbitMqEventBus>());

builder.Services.AddSingleton<IStepHandler, FtpScanHandler>();
builder.Services.AddSingleton<IStepHandler, ParseCsvHandler>();
builder.Services.AddSingleton<IStepHandler, LogSinkHandler>();
builder.Services.AddSingleton<IStepHandler, PostToHandler>();
builder.Services.AddSingleton<PipelineOrchestrator>();
builder.Services.AddHostedService<PipelineWorker>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// Лимит тела запроса (multipart) на каждый запрос.
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

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// FTP.
app.MapGet("/admin/ftp/files", () =>
{
    var files = AdminApiHelpers.FtpList(settings);
    return Results.Ok(files);
});

app.MapPost("/admin/ftp/upload", async ([FromForm] FtpUploadRequest req, ILogger<Program> logger, RabbitMqEventBus bus, IPipelineDefinitionProvider definitionProvider) =>
{
    if (req.File.Length == 0)
    {
        return Results.BadRequest("Файл пустой");
    }

    var name = req.File.FileName;
    logger.LogInformation("FTP upload start. name={Name} size={Size}", name, req.File.Length);
    await using var ms = new MemoryStream();
    await req.File.CopyToAsync(ms);
    ms.Position = 0;

    try
    {
        AdminApiHelpers.FtpUpload(settings, name, ms);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "FTP upload failed. name={Name}", name);
        throw;
    }
    logger.LogInformation("FTP upload done. name={Name}", name);
    var triggerTopic = definitionProvider.GetDefinition().GetFirstStep().TopicTag;
    var evt = new PipelineEvent(TopicTag.From(triggerTopic), Guid.NewGuid().ToString("N"), new Dictionary<string, string>
    {
        ["fileName"] = name
    });
    await bus.PublishAsync(evt);
    logger.LogInformation("FTP upload trigger published. topic={Topic}", triggerTopic);
    return Results.Ok(new { name, trigger = triggerTopic });
})
.WithOpenApi()
.DisableAntiforgery();

app.MapGet("/admin/ftp/download", (string name) =>
{
    var stream = AdminApiHelpers.FtpDownload(settings, name);
    return Results.File(stream, "application/octet-stream", name);
});

app.MapDelete("/admin/ftp/delete", (string name) =>
{
    AdminApiHelpers.FtpDelete(settings, name);
    return Results.Ok(new { name });
});

// S3.
app.MapGet("/admin/s3/objects", async (S3ObjectStorage s3, string? prefix) =>
{
    var keys = await s3.ListAsync(prefix, default);
    return Results.Ok(keys);
});

app.MapPost("/admin/s3/upload", async (S3ObjectStorage s3, [FromForm] S3UploadRequest req, ILogger<Program> logger) =>
{
    if (req.File.Length == 0)
    {
        return Results.BadRequest("Файл пустой");
    }

    logger.LogInformation("S3 upload start. key={Key} size={Size}", req.Key, req.File.Length);
    await using var stream = req.File.OpenReadStream();
    await s3.PutAsync(req.Key, stream, default);
    logger.LogInformation("S3 upload done. key={Key}", req.Key);
    return Results.Ok(new { key = req.Key });
})
.WithOpenApi()
.DisableAntiforgery();

app.MapGet("/admin/s3/download", async (S3ObjectStorage s3, string key) =>
{
    var stream = await s3.GetAsync(key, default);
    return Results.File(stream, "application/octet-stream", Path.GetFileName(key));
});

app.MapDelete("/admin/s3/delete", async (S3ObjectStorage s3, string key) =>
{
    await s3.DeleteAsync(key, default);
    return Results.Ok(new { key });
});

// RabbitMQ.
app.MapGet("/admin/rabbit/queues", async (IHttpClientFactory httpFactory) =>
{
    var queues = await AdminApiHelpers.RabbitApiGetAsync(settings, "/api/queues", httpFactory);
    var names = queues.Select(q => q.GetProperty("name").GetString() ?? "").Where(n => !string.IsNullOrWhiteSpace(n));
    return Results.Ok(names);
});

app.MapGet("/admin/rabbit/queues/stats", async (IHttpClientFactory httpFactory) =>
{
    var queues = await AdminApiHelpers.RabbitApiGetAsync(settings, "/api/queues", httpFactory);
    var stats = queues.Select(q => new
    {
        name = q.GetProperty("name").GetString() ?? "",
        messages = q.TryGetProperty("messages", out var msg) ? msg.GetInt32() : 0,
        consumers = q.TryGetProperty("consumers", out var cons) ? cons.GetInt32() : 0
    });
    return Results.Ok(stats);
});

app.MapPost("/admin/rabbit/queues/peek", async ([FromBody] RabbitPeekRequest req, IHttpClientFactory httpFactory) =>
{
    var vhost = settings.RabbitMq.VirtualHost == "/" ? "%2F" : settings.RabbitMq.VirtualHost;
    var path = $"/api/queues/{vhost}/{req.Queue}/get";
    var payload = new
    {
        count = req.Count,
        ackmode = "ack_requeue_true",
        encoding = "auto",
        truncate = 20000
    };

    var json = await AdminApiHelpers.RabbitApiPostAsync(settings, path, payload, httpFactory);
    var doc = JsonDocument.Parse(json);
    var result = doc.RootElement.EnumerateArray().Select(item => new
    {
        routingKey = item.TryGetProperty("routing_key", out var rk) ? rk.GetString() ?? "" : "",
        payload = item.TryGetProperty("payload", out var pl) ? pl.GetString() ?? "" : ""
    });

    return Results.Ok(result);
});

app.MapPost("/admin/rabbit/publish", async (RabbitPublishRequest req, RabbitMqEventBus bus) =>
{
    var payload = req.Payload ?? new Dictionary<string, string>();
    var evt = new PipelineEvent(TopicTag.From(req.Topic), Guid.NewGuid().ToString("N"), payload);
    await bus.PublishAsync(evt);
    return Results.Ok(new { topic = req.Topic, keys = payload.Keys.ToArray() });
});

// Consumer pause/resume.
app.MapPost("/admin/consumer/pause", () =>
{
    if (string.IsNullOrWhiteSpace(settings.Control?.PauseFilePath))
    {
        return Results.BadRequest("Control.PauseFilePath не задан");
    }

    Directory.CreateDirectory(Path.GetDirectoryName(settings.Control.PauseFilePath) ?? ".");
    File.WriteAllText(settings.Control.PauseFilePath, "pause");
    return Results.Ok(new { path = settings.Control.PauseFilePath });
});

app.MapPost("/admin/consumer/resume", () =>
{
    if (string.IsNullOrWhiteSpace(settings.Control?.PauseFilePath))
    {
        return Results.BadRequest("Control.PauseFilePath не задан");
    }

    if (File.Exists(settings.Control.PauseFilePath))
    {
        File.Delete(settings.Control.PauseFilePath);
    }

    return Results.Ok(new { path = settings.Control.PauseFilePath });
});

app.Run();

// Вспомогательные типы.
/// <summary>
/// Запрос на просмотр сообщений в очереди.
/// </summary>
public sealed record RabbitPeekRequest(string Queue, int Count);

/// <summary>
/// Запрос на публикацию события.
/// </summary>
public sealed record RabbitPublishRequest(string Topic, Dictionary<string, string>? Payload);

/// <summary>
/// Запрос на загрузку файла в FTP.
/// </summary>
public sealed record FtpUploadRequest(IFormFile File);

/// <summary>
/// Запрос на загрузку файла в S3.
/// </summary>
public sealed record S3UploadRequest(IFormFile File, string Key);

/// <summary>
/// Вспомогательные методы Admin API.
/// </summary>
internal static class AdminApiHelpers
{
    // FTP helpers.
    /// <summary>
    /// Создать FTP клиента.
    /// </summary>
    internal static FtpClient CreateFtpClient(PipelineSettings settings)
    {
        var ftp = settings.FtpConnection;
        var clientSettings = settings.FtpClient;
        var client = new FtpClient(ftp.Host)
        {
            Port = ftp.Port,
            Credentials = new NetworkCredential(ftp.User, ftp.Password)
        };

        client.Config.EncryptionMode = Enum.TryParse<FtpEncryptionMode>(clientSettings.EncryptionMode, true, out var enc)
            ? enc
            : FtpEncryptionMode.None;
        client.Config.DataConnectionType = Enum.TryParse<FtpDataConnectionType>(clientSettings.DataConnectionType, true, out var dataType)
            ? dataType
            : FtpDataConnectionType.PASV;
        client.Config.InternetProtocolVersions = Enum.TryParse<FtpIpVersion>(clientSettings.InternetProtocol, true, out var ipVer)
            ? ipVer
            : FtpIpVersion.IPv4;
        client.Config.ReadTimeout = clientSettings.ReadTimeoutMs;
        client.Config.ConnectTimeout = clientSettings.ConnectTimeoutMs;
        client.Config.SocketKeepAlive = clientSettings.SocketKeepAlive;

        if (ftp.UseSsl)
        {
            client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
            client.Config.ValidateAnyCertificate = true;
        }

        return client;
    }

    /// <summary>
    /// Нормализовать базовый путь FTP.
    /// </summary>
    internal static string NormalizeBasePath(PipelineSettings settings)
    {
        var basePath = settings.FtpDetection.SearchPath?.Trim() ?? string.Empty;
        basePath = basePath.Replace('\\', '/');
        if (string.IsNullOrWhiteSpace(basePath) || basePath == "/")
        {
            return "/";
        }
        if (!basePath.StartsWith('/'))
        {
            basePath = "/" + basePath;
        }
        if (basePath.EndsWith('/'))
        {
            basePath = basePath.TrimEnd('/');
        }
        return basePath;
    }

    /// <summary>
    /// Построить путь файла относительно базового каталога.
    /// </summary>
    internal static string BuildRemotePath(PipelineSettings settings, string name)
    {
        var basePath = settings.FtpDetection.SearchPath?.Trim() ?? string.Empty;
        if (basePath == "/")
        {
            basePath = string.Empty;
        }

        basePath = basePath.TrimStart('/');
        if (!string.IsNullOrWhiteSpace(basePath) && !basePath.EndsWith("/"))
        {
            basePath += "/";
        }

        return $"{basePath}{name.TrimStart('/')}";
    }


    /// <summary>
    /// Получить список файлов в FTP каталоге.
    /// </summary>
    internal static List<string> FtpList(PipelineSettings settings)
    {
        using var client = CreateFtpClient(settings);
        client.Connect();
        var basePath = NormalizeBasePath(settings);
        client.SetWorkingDirectory(basePath);
        var items = client.GetListing();
        return items
            .Where(i => i.Type == FtpObjectType.File)
            .Select(i => i.Name)
            .ToList();
    }

    /// <summary>
    /// Загрузить файл в FTP.
    /// </summary>
    internal static void FtpUpload(PipelineSettings settings, string remoteName, Stream content)
    {
        using var client = CreateFtpClient(settings);
        client.Connect();
        var basePath = NormalizeBasePath(settings);
        client.SetWorkingDirectory(basePath);
        var fileName = Path.GetFileName(remoteName);
        content.Position = 0;
        var status = client.UploadStream(content, fileName, FtpRemoteExists.Overwrite, true);
        if (status != FtpStatus.Success)
        {
            throw new InvalidOperationException($"FTP upload failed for {fileName}. Status={status}");
        }
    }

    /// <summary>
    /// Скачать файл из FTP.
    /// </summary>
    internal static Stream FtpDownload(PipelineSettings settings, string remoteName)
    {
        var ms = new MemoryStream();
        using var client = CreateFtpClient(settings);
        client.Connect();
        var basePath = NormalizeBasePath(settings);
        client.SetWorkingDirectory(basePath);
        var fileName = Path.GetFileName(remoteName);
        client.DownloadStream(ms, fileName);

        ms.Position = 0;
        return ms;
    }

    /// <summary>
    /// Удалить файл из FTP.
    /// </summary>
    internal static void FtpDelete(PipelineSettings settings, string remoteName)
    {
        using var client = CreateFtpClient(settings);
        client.Connect();
        var basePath = NormalizeBasePath(settings);
        client.SetWorkingDirectory(basePath);
        var fileName = Path.GetFileName(remoteName);
        client.DeleteFile(fileName);
    }

    // Rabbit helpers.
    /// <summary>
    /// Выполнить GET к RabbitMQ Management API.
    /// </summary>
    internal static async Task<List<JsonElement>> RabbitApiGetAsync(PipelineSettings settings, string path, IHttpClientFactory httpFactory)
    {
        var host = settings.RabbitMq.Host == "127.0.0.1" ? "127.0.0.1" : settings.RabbitMq.Host;
        var url = $"http://{host}:15672{path}";

        using var client = httpFactory.CreateClient();
        var auth = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{settings.RabbitMq.User}:{settings.RabbitMq.Password}"));
        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", auth);
        var json = await client.GetStringAsync(url);
        var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateArray().ToList();
    }

    /// <summary>
    /// Выполнить POST к RabbitMQ Management API.
    /// </summary>
    internal static async Task<string> RabbitApiPostAsync(PipelineSettings settings, string path, object payload, IHttpClientFactory httpFactory)
    {
        var host = settings.RabbitMq.Host == "127.0.0.1" ? "127.0.0.1" : settings.RabbitMq.Host;
        var url = $"http://{host}:15672{path}";

        using var client = httpFactory.CreateClient();
        var auth = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{settings.RabbitMq.User}:{settings.RabbitMq.Password}"));
        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", auth);
        var jsonPayload = JsonSerializer.Serialize(payload);
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await client.PostAsync(url, content);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync();
    }
}



