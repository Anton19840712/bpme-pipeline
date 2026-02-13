using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Encodings.Web;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Steps;

/// <summary>
/// Парсер CSV в JSON.
/// </summary>
public sealed class ParseCsvHandler : IStepHandler
{
    private readonly IObjectStorage _storage;
    private readonly IEventBus _bus;
    private readonly ILogger<ParseCsvHandler> _logger;
    private readonly PipelineTopicsSettings _topics;
    private readonly StoragePathsSettings _paths;

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public ParseCsvHandler(IObjectStorage storage, IEventBus bus, PipelineSettings settings, ILogger<ParseCsvHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _topics = settings.PipelineTopics;
        _paths = settings.StoragePaths;
        _logger = logger;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => TopicTag.From(_topics.ScanTopic);

    /// <summary>
    /// Обработать событие сканирования.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId });

        if (!evt.Payload.TryGetValue("s3Path", out var s3Path))
        {
            _logger.LogWarning("Missing s3Path in event payload");
            return;
        }

        var isDuplicate = evt.Payload.TryGetValue("isDuplicate", out var dup) && dup == "true";
        _logger.LogInformation("Parse start. s3={S3}", s3Path);

        await using var stream = await _storage.GetAsync(s3Path, ct);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var lines = new List<string>();
        while (!reader.EndOfStream)
        {
            lines.Add(await reader.ReadLineAsync() ?? string.Empty);
        }

        if (lines.Count == 0)
        {
            _logger.LogWarning("Empty CSV: {S3}", s3Path);
            return;
        }

        var headers = lines[0].Split(',').Select(CleanCsvCell).ToArray();
        var rows = new List<Dictionary<string, string>>();
        for (int i = 1; i < lines.Count; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                continue;
            }

            var cells = lines[i].Split(',').Select(CleanCsvCell).ToArray();
            var row = new Dictionary<string, string>();
            for (int c = 0; c < headers.Length && c < cells.Length; c++)
            {
                row[headers[c]] = cells[c];
            }
            rows.Add(row);
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

        var payload = new Dictionary<string, string>
        {
            ["parsedPath"] = jsonKey,
            ["rowsCount"] = rows.Count.ToString(),
            ["isDuplicate"] = isDuplicate ? "true" : "false"
        };

        // Технические события — просто лог/диагностика. Их можно публиковать после
        // каждого шага, но они не нужны для работы цепочки. Поэтому здесь публикуется
        // только переход к следующему шагу, чтобы не засорять шину.
        await _bus.PublishAsync(new PipelineEvent(TopicTag.From(_topics.ParsingTopic), evt.CorrelationId, payload), ct);
        _logger.LogInformation("Parse done. rows={Rows} s3={S3} duplicate={Duplicate}", rows.Count, jsonKey, isDuplicate);
    }
    private static string CleanCsvCell(string value)
    {
        var cell = value?.Trim() ?? string.Empty;
        if (cell.Length >= 2 && cell.StartsWith("\"") && cell.EndsWith("\""))
        {
            cell = cell[1..^1];
        }

        return cell.Replace("\"\"", "\"").Trim();
    }
}


