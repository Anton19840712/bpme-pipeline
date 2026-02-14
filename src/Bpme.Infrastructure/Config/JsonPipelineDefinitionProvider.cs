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
    private IReadOnlyList<PipelineDefinition>? _cached;

    /// <summary>
    /// Создать провайдер JSON пайплайна.
    /// </summary>
    public JsonPipelineDefinitionProvider(PipelineSettings settings, ILogger<JsonPipelineDefinitionProvider> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// Получить определения пайплайнов.
    /// </summary>
    public IReadOnlyList<PipelineDefinition> GetDefinitions()
    {
        if (_cached != null)
        {
            return _cached;
        }

        var files = new List<string>();
        if (_settings.PipelineDefinition.FileNames != null && _settings.PipelineDefinition.FileNames.Count > 0)
        {
            files.AddRange(_settings.PipelineDefinition.FileNames);
        }
        else if (!string.IsNullOrWhiteSpace(_settings.PipelineDefinition.FileName))
        {
            files.Add(_settings.PipelineDefinition.FileName);
        }
        else
        {
            throw new InvalidOperationException("PipelineDefinition.FileName или FileNames не задан.");
        }

        var result = new List<PipelineDefinition>();
        foreach (var file in files)
        {
            var fileName = file.Trim();
            if (string.IsNullOrWhiteSpace(fileName))
            {
                continue;
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
                throw new InvalidOperationException($"Не удалось десериализовать {fileName}.");
            }

            _logger.LogInformation("Pipeline definition loaded. Tag={Tag} Steps={Count} Path={Path}", definition.Tag, definition.Steps.Count, path);
            result.Add(definition);
        }

        _cached = result;
        return result;
    }
}
