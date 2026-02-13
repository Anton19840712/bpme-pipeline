using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Application.Pipeline;
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
    private readonly PipelineDefinition _definition;
    private readonly StoragePathsSettings _paths;
    private const string StepName = "parseCsvToJsonArray";

    /// <summary>
    /// Создать обработчик.
    /// </summary>
    public ParseCsvHandler(
        IObjectStorage storage,
        IEventBus bus,
        PipelineSettings settings,
        IPipelineDefinitionProvider definitionProvider,
        ILogger<ParseCsvHandler> logger)
    {
        _storage = storage;
        _bus = bus;
        _definition = definitionProvider.GetDefinition();
        _paths = settings.StoragePaths;
        _logger = logger;
    }

    /// <summary>
    /// Тема обработки.
    /// </summary>
    public TopicTag Topic => TopicTag.From(_definition.GetPreviousStep(StepName).TopicTag);

    /// <summary>
    /// Обработать событие сканирования.
    /// </summary>
    public async Task HandleAsync(PipelineEvent evt, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId });

        var step = _definition.GetStep(StepName);
        var delimiter = GetDelimiter(step);
        var hasHeader = GetBoolParam(step, "hasHeader", true);
        var encoding = GetEncoding(step);

        if (!evt.Payload.TryGetValue("s3Path", out var s3Path))
        {
            _logger.LogWarning("Missing s3Path in event payload");
            return;
        }

        var isDuplicate = evt.Payload.TryGetValue("isDuplicate", out var dup) && dup == "true";
        _logger.LogInformation("Parse start. s3={S3}", s3Path);

        await using var stream = await _storage.GetAsync(s3Path, ct);
        using var reader = new StreamReader(stream, encoding);

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

        var headerIndex = 0;
        string[] headers;
        if (hasHeader)
        {
            headers = lines[0].Split(delimiter).Select(CleanCsvCell).ToArray();
            headerIndex = 1;
        }
        else
        {
            var firstCells = lines[0].Split(delimiter);
            headers = Enumerable.Range(1, firstCells.Length).Select(i => $"col{i}").ToArray();
        }

        var rows = new List<Dictionary<string, string>>();
        for (int i = headerIndex; i < lines.Count; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                continue;
            }

            var cells = lines[i].Split(delimiter).Select(CleanCsvCell).ToArray();
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
        await _bus.PublishAsync(new PipelineEvent(TopicTag.From(_definition.GetStep(StepName).TopicTag), evt.CorrelationId, payload), ct);
        _logger.LogInformation("Parse done. rows={Rows} s3={S3} duplicate={Duplicate}", rows.Count, jsonKey, isDuplicate);
    }

    /// <summary>
    /// Нормализовать значение ячейки CSV.
    /// </summary>
    private static string CleanCsvCell(string value)
    {
        var cell = value?.Trim() ?? string.Empty;
        if (cell.Length >= 2 && cell.StartsWith("\"") && cell.EndsWith("\""))
        {
            cell = cell[1..^1];
        }

        return cell.Replace("\"\"", "\"").Trim();
    }

    /// <summary>
    /// Получить параметр шага.
    /// </summary>
    private static string? GetParam(PipelineStep step, string key)
    {
        return step.GetParameter(key);
    }

    /// <summary>
    /// Получить bool параметр шага.
    /// </summary>
    private static bool GetBoolParam(PipelineStep step, string key, bool defaultValue)
    {
        var raw = GetParam(step, key);
        return bool.TryParse(raw, out var value) ? value : defaultValue;
    }

    /// <summary>
    /// Получить разделитель CSV.
    /// </summary>
    private static char GetDelimiter(PipelineStep step)
    {
        var raw = GetParam(step, "delimiter");
        return !string.IsNullOrWhiteSpace(raw) ? raw[0] : ',';
    }

    /// <summary>
    /// Получить кодировку CSV.
    /// </summary>
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
}
