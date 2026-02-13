using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Application.Pipeline;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using FluentFTP;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;

/// <summary>
/// Шаг сканирования FTP.
/// </summary>
public sealed class FtpScanHandler : IStepHandler
{
    private readonly IObjectStorage _storage;
    private readonly IEventBus _bus;
    private readonly IStateStore _stateStore;
    private readonly PipelineSettings _settings;
    private readonly ILogger<FtpScanHandler> _logger;
    private readonly PipelineDefinition _definition;
    private readonly StoragePathsSettings _paths;
    private readonly FtpClientSettings _ftpClientSettings;
    private const string StepName = "ftpScan";

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public FtpScanHandler(
        IObjectStorage storage,
        IEventBus bus,
        IStateStore stateStore,
        PipelineSettings settings,
        IPipelineDefinitionProvider definitionProvider,
        ILogger<FtpScanHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _stateStore = stateStore;
        _settings = settings;
        _definition = definitionProvider.GetDefinition();
        _paths = settings.StoragePaths;
        _ftpClientSettings = settings.FtpClient;
        _logger = logger;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => TopicTag.From(_definition.GetPreviousStep(StepName).TopicTag);

    /// <summary>
    /// Обработать событие триггера.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId });

        try
        {
            var ftp = _settings.FtpConnection;
            var detect = _settings.FtpDetection;
            var step = _definition.GetStep(StepName);

            var host = ftp.Host;
            var port = ftp.Port;
            ApplyFtpOverrides(step, ref host, ref port);
            var user = GetParam(step, "user") ?? ftp.User;
            var password = GetParam(step, "password") ?? ftp.Password;
            var searchPath = GetParam(step, "searchPath") ?? detect.SearchPath;
            var mask = GetParam(step, "byMask") ?? detect.Mask;
            var notOlder = GetIntParam(step, "NotOlderThanInSeconds")
                           ?? GetIntParam(step, "NotOlderThenInSeconds")
                           ?? detect.NotOlderThanSeconds;

            _logger.LogInformation("FTP scan start. Host={Host} Path={Path}", host, searchPath);

            var basePath = searchPath?.Trim();
            if (string.IsNullOrWhiteSpace(basePath))
            {
                basePath = "/";
            }

            if (!basePath.EndsWith("/"))
            {
                basePath += "/";
            }

            using var client = CreateFtpClient(host, port, user, password, ftp.UseSsl, _ftpClientSettings);
            client.Connect();

            var items = client.GetListing(basePath);
            var names = items
                .Where(i => i.Type == FtpObjectType.File)
                .Select(i => i.Name)
                .ToList();

            var now = DateTimeOffset.UtcNow;
            var cutoff = now.AddSeconds(-notOlder);
            var regex = WildcardToRegex(mask);

            var files = new List<(string Name, long Size, DateTime ModifiedUtc)>();

            foreach (var name in names.Where(n => regex.IsMatch(n)))
            {
                var fullPath = BuildRemotePath(basePath, name);
                var size = client.GetFileSize(fullPath);
                var modifiedUtc = client.GetModifiedTime(fullPath).ToUniversalTime();

                if (modifiedUtc >= cutoff.UtcDateTime)
                {
                    files.Add((name, size, modifiedUtc));
                }
            }

            _logger.LogInformation("FTP files matched: {Count}", files.Count);

            // Если событие содержит конкретный файл — обрабатываем только его.
            if (evt.Payload.TryGetValue("fileName", out var targetName) && !string.IsNullOrWhiteSpace(targetName))
            {
                files = files.Where(f => string.Equals(f.Name, targetName, StringComparison.OrdinalIgnoreCase)).ToList();
                _logger.LogInformation("FTP scan filtered by fileName. Target={Name} matched={Count}", targetName, files.Count);
            }

            foreach (var file in files)
            {
                _logger.LogInformation("FTP file: {Name} size={Size} modified={Modified}", file.Name, file.Size, file.ModifiedUtc);

                if (detect.StableForSeconds > 0)
                {
                    var size1 = file.Size;
                    await Task.Delay(TimeSpan.FromSeconds(detect.StableForSeconds), ct);
                    var fullPath = BuildRemotePath(basePath, file.Name);
                    var size2 = client.GetFileSize(fullPath);
                    if (size2 != size1)
                    {
                        _logger.LogInformation("File is still changing: {Name}", file.Name);
                        continue;
                    }
                }

                await using var ms = new MemoryStream();
                var downloadPath = BuildRemotePath(basePath, file.Name);
                var ok = client.DownloadStream(ms, downloadPath);
                if (!ok)
                {
                    _logger.LogWarning("Failed to download: {Name}", file.Name);
                    continue;
                }

                ms.Position = 0;
                var hash = ComputeSha256(ms);
                ms.Position = 0;

                var isDuplicate = await _stateStore.IsProcessedAsync(hash, ct);
                var dedupMode = _settings.FtpDetection.Deduplication.Mode?.Trim().ToLowerInvariant() ?? "skip";
                var hasTarget = evt.Payload.TryGetValue("fileName", out var targetName2) && !string.IsNullOrWhiteSpace(targetName2);
                if (isDuplicate)
                {
                    if (dedupMode == "emit" && hasTarget)
                    {
                        _logger.LogInformation("Duplicate file detected (emit for target). Name={Name} hash={Hash}", file.Name, hash);
                    }
                    else
                    {
                        _logger.LogInformation("File already processed (skip by policy). Name={Name} hash={Hash} policy={Policy}", file.Name, hash, dedupMode);
                        continue;
                    }
                }

                var fileId = FileId.From(hash);
                var s3Key = $"{_paths.RawPrefix}{fileId.Value}.csv";
                await _storage.PutAsync(s3Key, ms, ct);

                var payload = new Dictionary<string, string>
                {
                    ["fileId"] = fileId.Value,
                    ["s3Path"] = s3Key,
                    ["fileName"] = file.Name,
                    ["isDuplicate"] = isDuplicate ? "true" : "false"
                };

                // Технические события — просто лог/диагностика. Их можно публиковать после
                // каждого шага, но они не нужны для работы цепочки. Поэтому здесь публикуется
                // только переход к следующему шагу, чтобы не засорять шину.
                await _bus.PublishAsync(new PipelineEvent(TopicTag.From(_definition.GetStep(StepName).TopicTag), evt.CorrelationId, payload), ct);
                if (!isDuplicate)
                {
                    await _stateStore.MarkProcessedAsync(fileId.Value, ct);
                }

                _logger.LogInformation("File processed: {Name} s3={S3} duplicate={Duplicate}", file.Name, s3Key, isDuplicate);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FTP scan failed");
        }
    }

    /// <summary>
    /// Создать FTP клиента с параметрами подключения.
    /// </summary>
    /// <summary>
    /// Создать FTP клиента с параметрами подключения.
    /// </summary>
    private static FtpClient CreateFtpClient(string host, int port, string user, string password, bool useSsl, FtpClientSettings clientSettings)
    {
        var client = new FtpClient(host)
        {
            Port = port,
            Credentials = new System.Net.NetworkCredential(user, password)
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

        if (useSsl)
        {
            client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
            client.Config.ValidateAnyCertificate = true;
        }

        return client;
    }

    /// <summary>
    /// Применить параметры подключения из шага.
    /// </summary>
    private static void ApplyFtpOverrides(PipelineStep step, ref string host, ref int port)
    {
        var hostRaw = GetParam(step, "host");
        if (string.IsNullOrWhiteSpace(hostRaw))
        {
            return;
        }

        var cleaned = hostRaw.Trim();
        if (cleaned.Contains("://", StringComparison.OrdinalIgnoreCase))
        {
            if (Uri.TryCreate(cleaned, UriKind.Absolute, out var uri))
            {
                host = uri.Host;
                if (uri.Port > 0)
                {
                    port = uri.Port;
                }
                return;
            }
        }

        var parts = cleaned.Split(':', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 1)
        {
            host = parts[0];
        }
        if (parts.Length >= 2 && int.TryParse(parts[1], out var parsedPort))
        {
            port = parsedPort;
        }
    }

    /// <summary>
    /// Получить параметр шага.
    /// </summary>
    private static string? GetParam(PipelineStep step, string key)
    {
        return step.GetParameter(key);
    }

    /// <summary>
    /// Получить числовой параметр шага.
    /// </summary>
    private static int? GetIntParam(PipelineStep step, string key)
    {
        var raw = GetParam(step, key);
        return int.TryParse(raw, out var value) ? value : null;
    }

    /// <summary>
    /// Построить путь на FTP с учётом базового каталога.
    /// </summary>
    private static string BuildRemotePath(string basePath, string name)
    {
        var path = basePath?.Trim() ?? string.Empty;
        if (path == "/")
        {
            path = string.Empty;
        }

        path = path.TrimStart('/');
        if (!string.IsNullOrWhiteSpace(path) && !path.EndsWith("/"))
        {
            path += "/";
        }

        return $"{path}{name.TrimStart('/')}";
    }

    /// <summary>
    /// Посчитать SHA-256 от содержимого потока.
    /// </summary>
    private static string ComputeSha256(Stream stream)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Преобразовать маску вида *.csv в регулярное выражение.
    /// </summary>
    private static Regex WildcardToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".");
        return new Regex($"^{escaped}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    }
}
