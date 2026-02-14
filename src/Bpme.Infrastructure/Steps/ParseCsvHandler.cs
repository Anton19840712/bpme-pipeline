using System.Text;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Application.Pipeline;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;
public sealed class ParseCsvHandler : IStepHandler, IMultiTopicStepHandler, IStepNameProvider
{
    private readonly IObjectStorage _storage;
    private readonly IEventBus _bus;
    private readonly ILogger<ParseCsvHandler> _logger;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly StoragePathsSettings _paths;
    private readonly PipelineSettings _settings;
    private const string StepNameConst = "parseCsvToJsonArray";
    public ParseCsvHandler(
        IObjectStorage storage,
        IEventBus bus,
        PipelineSettings settings,
        IPipelineDefinitionRegistry registry,
        ILogger<ParseCsvHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _registry = registry;
        _paths = settings.StoragePaths;
        _settings = settings;
        _logger = logger;
    }
    public TopicTag Topic => Topics.Count > 0 ? Topics[0] : TopicTag.From(_settings.PipelineTopics.ScanTopic);
    public IReadOnlyList<TopicTag> Topics => ResolveTopics();
    public string StepName => StepNameConst;
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
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

        var delimiter = GetDelimiter(step);
        var hasHeader = GetBoolParam(step, "hasHeader", true);
        var encoding = GetEncoding(step);

        if (!evt.Payload.TryGetValue("s3Path", out var s3Path))
        {
            _logger.LogWarning("Missing s3Path in event payload");
            _logger.LogInformation("статус=finished");
            return;
        }

        var isDuplicate = evt.Payload.TryGetValue("isDuplicate", out var dup) && dup == "true";
        _logger.LogInformation("Parse start. s3={S3}", s3Path);

        await using var stream = await _storage.GetAsync(s3Path, ct);
        using var reader = new StreamReader(stream, encoding);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = delimiter.ToString(),
            HasHeaderRecord = hasHeader,
            IgnoreBlankLines = true,
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null,
            MissingFieldFound = null
        };

        using var csv = new CsvReader(reader, csvConfig);

        var rows = new List<Dictionary<string, string>>();
        string[] headers;

        if (hasHeader)
        {
            if (!csv.Read())
            {
                _logger.LogWarning("Empty CSV: {S3}", s3Path);
                _logger.LogInformation("статус=finished");
                return;
            }

            csv.ReadHeader();
            headers = csv.HeaderRecord ?? Array.Empty<string>();
            headers = NormalizeHeaders(headers);

            while (csv.Read())
            {
                var row = new Dictionary<string, string>();
                for (int i = 0; i < headers.Length; i++)
                {
                    row[headers[i]] = csv.GetField(i) ?? string.Empty;
                }
                rows.Add(row);
            }
        }
        else
        {
            headers = Array.Empty<string>();
            while (csv.Read())
            {
                var record = csv.Parser.Record ?? Array.Empty<string>();
                if (record.Length == 0)
                {
                    continue;
                }

                if (headers.Length == 0)
                {
                    headers = Enumerable.Range(1, record.Length).Select(i => $"col{i}").ToArray();
                }

                var row = new Dictionary<string, string>();
                for (int i = 0; i < headers.Length && i < record.Length; i++)
                {
                    row[headers[i]] = record[i] ?? string.Empty;
                }
                rows.Add(row);
            }
        }

        var json = JsonSerializer.Serialize(rows, new JsonSerializerOptions
        {
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        });
        var jsonBytes = Encoding.UTF8.GetBytes(json);
        var jsonKey = $"{_paths.ParsedPrefix}{Guid.NewGuid():N}.json";
        await using var jsonStream = new MemoryStream(jsonBytes);
        await _storage.PutAsync(jsonKey, jsonStream, ct);

        var payload = new Dictionary<string, string>(evt.Payload)
        {
            ["parsedPath"] = jsonKey,
            ["rowsCount"] = rows.Count.ToString(),
            ["isDuplicate"] = isDuplicate ? "true" : "false",
            ["Process"] = definition.Tag
        };

        await _bus.PublishAsync(new PipelineEvent(TopicTag.From(step.TopicTag), evt.CorrelationId, payload), ct);
        _logger.LogInformation("Parse done. rows={Rows} s3={S3} duplicate={Duplicate} tag={Tag}", rows.Count, jsonKey, isDuplicate, definition.Tag);

        _logger.LogInformation("статус=finished");

        if (_registry.IsLastStepByInputTopic(definition, StepNameConst, evt.Topic.Value))
        {
            _logger.LogInformation("процесс завершён");
        }
    }
    private static string[] NormalizeHeaders(string[] headers)
    {
        var result = new string[headers.Length];
        for (int i = 0; i < headers.Length; i++)
        {
            var header = headers[i]?.Trim() ?? string.Empty;
            result[i] = string.IsNullOrWhiteSpace(header) ? $"col{i + 1}" : header;
        }

        return result;
    }
    private static string? GetParam(PipelineStep step, string key)
    {
        return step.GetParameter(key);
    }
    private static bool GetBoolParam(PipelineStep step, string key, bool defaultValue)
    {
        var raw = GetParam(step, key);
        return bool.TryParse(raw, out var value) ? value : defaultValue;
    }
    private static char GetDelimiter(PipelineStep step)
    {
        var raw = GetParam(step, "delimiter");
        return !string.IsNullOrWhiteSpace(raw) ? raw[0] : ',';
    }
    private static Encoding GetEncoding(PipelineStep step)
    {
        var raw = GetParam(step, "encoding");
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Encoding.UTF8;
        }

        try
        {
            return Encoding.GetEncoding(raw);
        }
        catch
        {
            return Encoding.UTF8;
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
}






