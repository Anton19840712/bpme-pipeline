using Bpme.Application.Pipeline;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Реестр определений пайплайнов.
/// </summary>
public interface IPipelineDefinitionRegistry
{
    /// <summary>
    /// Получить все определения.
    /// </summary>
    IReadOnlyList<PipelineDefinition> GetAll();

    /// <summary>
    /// Найти определение по тегу.
    /// </summary>
    PipelineDefinition GetByTag(string tag);

    /// <summary>
    /// Получить первый шаг.
    /// </summary>
    PipelineStep GetFirstStep(PipelineDefinition definition);

    /// <summary>
    /// Получить шаг по имени.
    /// </summary>
    PipelineStep GetStep(PipelineDefinition definition, string name);

    /// <summary>
    /// Получить шаг по имени и входной теме.
    /// </summary>
    PipelineStep GetStepByInputTopic(PipelineDefinition definition, string name, string inputTopic);

    /// <summary>
    /// Получить предыдущий шаг.
    /// </summary>
    PipelineStep GetPreviousStep(PipelineDefinition definition, string name);

    /// <summary>
    /// Получить входные темы для всех вхождений шага.
    /// </summary>
    IReadOnlyList<string> GetInputTopics(PipelineDefinition definition, string stepName);

    /// <summary>
    /// Разрешить определение по входной теме шага.
    /// </summary>
    PipelineDefinition ResolveByInputTopic(string stepName, string inputTopic);

    /// <summary>
    /// Разрешить определение по теме и тегу пайплайна.
    /// </summary>
    PipelineDefinition ResolveByInputTopic(string stepName, string inputTopic, string? pipelineTag);

    /// <summary>
    /// Определить, является ли шаг последним для входной темы.
    /// </summary>
    bool IsLastStepByInputTopic(PipelineDefinition definition, string stepName, string inputTopic);
}
