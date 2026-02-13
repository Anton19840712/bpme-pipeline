using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Config;

/// <summary>
/// Провайдер определения пайплайна из JSON файла.
/// </summary>
public sealed class JsonPipelineDefinitionProvider : IPipelineDefinitionProvider
{
    private readonly PipelineSettings _settings;
    private readonly ILogger<JsonPipelineDefinitionProvider> _logger;
    private PipelineDefinition? _cached;

    /// <summary>
    /// Создать провайдер JSON пайплайна.
    /// </summary>
    public JsonPipelineDefinitionProvider(PipelineSettings settings, ILogger<JsonPipelineDefinitionProvider> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// Получить определение пайплайна.
    /// </summary>
    public PipelineDefinition GetDefinition()
    {
        if (_cached != null)
        {
            return _cached;
        }

        var fileName = _settings.PipelineDefinition.FileName?.Trim();
        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new InvalidOperationException("PipelineDefinition.FileName не задан.");
        }

        var path = Path.IsPathRooted(fileName)
            ? fileName
            : Path.Combine(AppContext.BaseDirectory, fileName);

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Файл определения пайплайна не найден: {path}");
        }

        var json = File.ReadAllText(path);
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var definition = JsonSerializer.Deserialize<PipelineDefinition>(json, options);
        if (definition == null)
        {
            throw new InvalidOperationException("Не удалось десериализовать pipeline.json.");
        }

        _logger.LogInformation("Pipeline definition loaded. Tag={Tag} Steps={Count} Path={Path}", definition.Tag, definition.Steps.Count, path);
        _cached = definition;
        return definition;
    }
}
