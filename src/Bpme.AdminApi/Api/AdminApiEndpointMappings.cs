using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.AspNetCore.Mvc;

namespace Bpme.AdminApi;

internal static class AdminApiEndpointMappings
{
    public static WebApplication MapAdminApi(this WebApplication app, PipelineSettings settings)
    {
        #region Health
        app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
        #endregion

        #region FTP
        app.MapGet("/admin/ftp/files", (IFtpAdminClient ftp) =>
        {
            var files = ftp.List();
            return Results.Ok(files);
        })
        .WithTags("FTP");

        app.MapPost("/admin/ftp/upload", async ([FromForm] FtpUploadRequest req, FtpIngressService ftpIngress, string? processTag, CancellationToken ct) =>
        {
            if (settings.FtpDetection.TriggerMode == TriggerMode.Background)
            {
                return Results.Conflict("Manual trigger disabled by TriggerMode=Background.");
            }

            if (req.File.Length == 0)
            {
                return Results.BadRequest("File is empty");
            }

            var name = req.File.FileName;
            await using var ms = new MemoryStream();
            await req.File.CopyToAsync(ms, ct);
            ms.Position = 0;
            var result = await ftpIngress.UploadAndTriggerAsync(name, ms, processTag, ct);
            return Results.Ok(new { name = result.FileName, processes = result.Processes });
        })
        .WithOpenApi()
        .DisableAntiforgery()
        .WithTags("FTP");

        app.MapGet("/admin/ftp/download", (string name, IFtpAdminClient ftp) =>
        {
            var stream = ftp.Download(name);
            return Results.File(stream, "application/octet-stream", name);
        })
        .WithTags("FTP");

        app.MapDelete("/admin/ftp/delete", (string name, IFtpAdminClient ftp) =>
        {
            ftp.Delete(name);
            return Results.Ok(new { name });
        })
        .WithTags("FTP");

        app.MapPost("/admin/ftp/clear", (IFtpAdminClient ftp) =>
        {
            var deleted = ftp.Clear();
            return Results.Ok(new { deleted });
        })
        .WithTags("FTP");
        #endregion

        #region S3
        app.MapGet("/admin/s3/objects", async (IObjectStorage storage, string? prefix) =>
        {
            var keys = await storage.ListAsync(prefix, default);
            return Results.Ok(keys);
        })
        .WithTags("S3");

        app.MapPost("/admin/s3/upload", async (IObjectStorage storage, [FromForm] S3UploadRequest req, ILogger<Program> logger) =>
        {
            if (req.File.Length == 0)
            {
                return Results.BadRequest("File is empty");
            }

            logger.LogInformation("S3 upload start. key={Key} size={Size}", req.Key, req.File.Length);
            await using var stream = req.File.OpenReadStream();
            await storage.PutAsync(req.Key, stream, default);
            logger.LogInformation("S3 upload done. key={Key}", req.Key);
            return Results.Ok(new { key = req.Key });
        })
        .WithOpenApi()
        .DisableAntiforgery()
        .WithTags("S3");

        app.MapGet("/admin/s3/download", async (IObjectStorage storage, string key) =>
        {
            var stream = await storage.GetAsync(key, default);
            return Results.File(stream, "application/octet-stream", Path.GetFileName(key));
        })
        .WithTags("S3");

        app.MapDelete("/admin/s3/delete", async (IObjectStorage storage, string key) =>
        {
            await storage.DeleteAsync(key, default);
            return Results.Ok(new { key });
        })
        .WithTags("S3");

        app.MapPost("/admin/s3/clear", async (IObjectStorage storage, string? prefix) =>
        {
            var keys = await storage.ListAsync(prefix, default);
            foreach (var key in keys)
            {
                await storage.DeleteAsync(key, default);
            }

            var deleted = keys.Count;
            return Results.Ok(new { deleted, prefix = prefix ?? "" });
        })
        .WithTags("S3");
        #endregion

        #region RabbitMQ
        app.MapGet("/admin/rabbit/queues", async (IRabbitMqManagementClient rabbitManagement) =>
        {
            var queues = await rabbitManagement.GetQueuesAsync();
            var names = queues.Select(q => q.GetProperty("name").GetString() ?? "").Where(n => !string.IsNullOrWhiteSpace(n));
            return Results.Ok(names);
        })
        .WithTags("RabbitMQ");

        app.MapGet("/admin/rabbit/queues/stats", async (IRabbitMqManagementClient rabbitManagement) =>
        {
            var queues = await rabbitManagement.GetQueuesAsync();
            var stats = queues.Select(q => new
            {
                name = q.GetProperty("name").GetString() ?? "",
                messages = q.TryGetProperty("messages", out var msg) ? msg.GetInt32() : 0,
                consumers = q.TryGetProperty("consumers", out var cons) ? cons.GetInt32() : 0
            });
            return Results.Ok(stats);
        })
        .WithTags("RabbitMQ");

        app.MapPost("/admin/rabbit/queues/peek", async ([FromBody] RabbitPeekRequest req, IRabbitMqManagementClient rabbitManagement) =>
        {
            var json = await rabbitManagement.PeekAsync(req.Queue, req.Count);
            var doc = JsonDocument.Parse(json);
            var result = doc.RootElement.EnumerateArray().Select(item => new
            {
                routingKey = item.TryGetProperty("routing_key", out var rk) ? rk.GetString() ?? "" : "",
                payload = item.TryGetProperty("payload", out var pl) ? pl.GetString() ?? "" : ""
            });

            return Results.Ok(result);
        })
        .WithTags("RabbitMQ");

        app.MapPost("/admin/rabbit/publish", async (RabbitPublishRequest req, IEventBus bus) =>
        {
            var payload = req.Payload ?? new Dictionary<string, string>();
            var evt = new PipelineEvent(TopicTag.From(req.Topic), Guid.NewGuid().ToString("N"), payload);
            await bus.PublishAsync(evt);
            return Results.Ok(new { topic = req.Topic, keys = payload.Keys.ToArray() });
        })
        .WithTags("RabbitMQ");
        #endregion

        return app;
    }
}
