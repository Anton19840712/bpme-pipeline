using Bpme.Application.Abstractions;

namespace Bpme.Application.Pipeline;

/// <summary>
/// Реестр определений пайплайнов.
/// </summary>
public sealed class PipelineDefinitionRegistry : IPipelineDefinitionRegistry
{
    private readonly IPipelineDefinitionProvider _provider;
    private IReadOnlyList<PipelineDefinition>? _cached;

    /// <summary>
    /// Создать реестр.
    /// </summary>
    public PipelineDefinitionRegistry(IPipelineDefinitionProvider provider)
    {
        _provider = provider;
    }

    /// <summary>
    /// Получить все определения.
    /// </summary>
    public IReadOnlyList<PipelineDefinition> GetAll()
    {
        if (_cached != null)
        {
            return _cached;
        }

        var all = _provider.GetDefinitions();
        _cached = all.Where(d => d.Enabled).ToList();
        ValidateTopics(_cached);
        return _cached;
    }

    /// <summary>
    /// Найти определение по тегу.
    /// </summary>
    public PipelineDefinition GetByTag(string tag)
    {
        var definition = GetAll().FirstOrDefault(d => string.Equals(d.Tag, tag, StringComparison.OrdinalIgnoreCase));
        if (definition == null)
        {
            throw new InvalidOperationException($"Pipeline '{tag}' не найден.");
        }

        return definition;
    }

    /// <summary>
    /// Получить первый шаг.
    /// </summary>
    public PipelineStep GetFirstStep(PipelineDefinition definition)
    {
        return definition.GetFirstStep();
    }

    /// <summary>
    /// Получить шаг по имени.
    /// </summary>
    public PipelineStep GetStep(PipelineDefinition definition, string name)
    {
        return definition.GetStep(name);
    }

    /// <summary>
    /// Получить шаг по имени и входной теме.
    /// </summary>
    public PipelineStep GetStepByInputTopic(PipelineDefinition definition, string name, string inputTopic)
    {
        return definition.GetStepByInputTopic(name, inputTopic);
    }

    /// <summary>
    /// Получить предыдущий шаг.
    /// </summary>
    public PipelineStep GetPreviousStep(PipelineDefinition definition, string name)
    {
        return definition.GetPreviousStep(name);
    }

    /// <summary>
    /// Получить входные темы для всех вхождений шага.
    /// </summary>
    public IReadOnlyList<string> GetInputTopics(PipelineDefinition definition, string stepName)
    {
        return definition.GetInputTopics(stepName);
    }

    /// <summary>
    /// Разрешить определение по входной теме шага.
    /// </summary>
    public PipelineDefinition ResolveByInputTopic(string stepName, string inputTopic)
    {
        PipelineDefinition? match = null;
        foreach (var definition in GetAll())
        {
            try
            {
                definition.GetStepByInputTopic(stepName, inputTopic);
                if (match != null)
                {
                    throw new InvalidOperationException($"Тема '{inputTopic}' совпадает в нескольких пайплайнах.");
                }

                match = definition;
            }
            catch (InvalidOperationException)
            {
                // шаг отсутствует в конкретном пайплайне
            }
        }

        if (match == null)
        {
            throw new InvalidOperationException($"Не найден пайплайн для темы '{inputTopic}' и шага '{stepName}'.");
        }

        return match;
    }

    /// <summary>
    /// Разрешить определение по входной теме и тегу пайплайна.
    /// </summary>
    public PipelineDefinition ResolveByInputTopic(string stepName, string inputTopic, string? pipelineTag)
    {
        if (!string.IsNullOrWhiteSpace(pipelineTag))
        {
            var definition = GetByTag(pipelineTag);
            definition.GetStepByInputTopic(stepName, inputTopic);
            return definition;
        }

        return ResolveByInputTopic(stepName, inputTopic);
    }

    /// <summary>
    /// Определить, является ли шаг последним для входной темы.
    /// </summary>
    public bool IsLastStepByInputTopic(PipelineDefinition definition, string stepName, string inputTopic)
    {
        return definition.IsLastStepByInputTopic(stepName, inputTopic);
    }

    private static void ValidateTopics(IReadOnlyList<PipelineDefinition> definitions)
    {
        var used = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var definition in definitions)
        {
            foreach (var step in definition.Steps)
            {
                if (string.IsNullOrWhiteSpace(step.TopicTag))
                {
                    throw new InvalidOperationException($"Пустой topicTag в пайплайне '{definition.Tag}'.");
                }

                if (used.TryGetValue(step.TopicTag, out var existing))
                {
                    throw new InvalidOperationException($"Конфликт topicTag '{step.TopicTag}' между '{existing}' и '{definition.Tag}'.");
                }

                used[step.TopicTag] = definition.Tag;
            }
        }
    }
}
