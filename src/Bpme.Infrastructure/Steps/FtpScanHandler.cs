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
public sealed class FtpScanHandler : IStepHandler, IMultiTopicStepHandler, IStepNameProvider
{
    private readonly IObjectStorage _storage;
    private readonly IEventBus _bus;
    private readonly IStateStore _stateStore;
    private readonly PipelineSettings _settings;
    private readonly ILogger<FtpScanHandler> _logger;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly StoragePathsSettings _paths;
    private readonly FtpClientSettings _ftpClientSettings;
    private const string StepNameConst = "ftpScan";
    public FtpScanHandler(
        IObjectStorage storage,
        IEventBus bus,
        IStateStore stateStore,
        PipelineSettings settings,
        IPipelineDefinitionRegistry registry,
        ILogger<FtpScanHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _stateStore = stateStore;
        _settings = settings;
        _registry = registry;
        _paths = settings.StoragePaths;
        _ftpClientSettings = settings.FtpClient;
        _logger = logger;
    }
    public TopicTag Topic => Topics.Count > 0 ? Topics[0] : TopicTag.From(_settings.PipelineTopics.TriggerTopic);
    public IReadOnlyList<TopicTag> Topics => ResolveTopics();
    public string StepName => StepNameConst;
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        try
        {
            var ftp = _settings.FtpConnection;
            var detect = _settings.FtpDetection;
            var pipelineTag = evt.Payload.TryGetValue("pipelineTag", out var tag) ? tag : null;
            var iteration = evt.Payload.TryGetValue("iteration", out var iter) ? iter : "-";
            var definition = _registry.ResolveByInputTopic(StepNameConst, evt.Topic.Value, pipelineTag);
            var step = _registry.GetStepByInputTopic(definition, StepNameConst, evt.Topic.Value);

            using var scope = _logger.BeginScope(new Dictionary<string, object>
            {
                ["correlationId"] = evt.CorrelationId,
                ["Process"] = definition.Tag,
                ["Step"] = StepNameConst,
                ["Iteration"] = iteration
            });

            _logger.LogInformation("статус=started");

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
            var hasTarget = evt.Payload.TryGetValue("fileName", out var targetName) && !string.IsNullOrWhiteSpace(targetName);

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
                if (hasTarget && !string.Equals(name, targetName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var fullPath = BuildRemotePath(basePath, name);
                var size = client.GetFileSize(fullPath);
                var modifiedUtc = client.GetModifiedTime(fullPath).ToUniversalTime();

                if (hasTarget || modifiedUtc >= cutoff.UtcDateTime)
                {
                    files.Add((name, size, modifiedUtc));
                }
            }

            _logger.LogInformation("FTP files matched: {Count}", files.Count);
            if (files.Count == 0)
            {
                var available = string.Join(", ", names);
                _logger.LogInformation("FTP files available in {Path}: {Names}", basePath, available);
            }

            if (hasTarget)
            {
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

                var payload = new Dictionary<string, string>(evt.Payload)
                {
                    ["fileId"] = fileId.Value,
                    ["s3Path"] = s3Key,
                    ["fileName"] = file.Name,
                    ["isDuplicate"] = isDuplicate ? "true" : "false",
                    ["Process"] = definition.Tag
                };

                await _bus.PublishAsync(new PipelineEvent(TopicTag.From(step.TopicTag), evt.CorrelationId, payload), ct);
                if (!isDuplicate)
                {
                    await _stateStore.MarkProcessedAsync(fileId.Value, ct);
                }

                _logger.LogInformation("File processed: {Name} s3={S3} duplicate={Duplicate} tag={Tag}", file.Name, s3Key, isDuplicate, definition.Tag);
            }

            _logger.LogInformation("статус=finished");

            if (_registry.IsLastStepByInputTopic(definition, StepNameConst, evt.Topic.Value))
            {
                _logger.LogInformation("процесс завершён");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FTP scan failed");
        }
    }

    private IReadOnlyList<TopicTag> ResolveTopics()
    {
        var topics = new List<TopicTag>();
        foreach (var definition in _registry.GetAll())
        {
            var inputTopics = _registry.GetInputTopics(definition, StepNameConst);
            foreach (var input in inputTopics)
            {
                topics.Add(TopicTag.From(input));
            }
        }

        return topics;
    }
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
    private static string? GetParam(PipelineStep step, string key)
    {
        return step.GetParameter(key);
    }
    private static int? GetIntParam(PipelineStep step, string key)
    {
        var raw = GetParam(step, key);
        return int.TryParse(raw, out var value) ? value : null;
    }
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
    private static string ComputeSha256(Stream stream)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
    private static Regex WildcardToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".");
        return new Regex($"^{escaped}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    }
}






