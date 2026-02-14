using Bpme.Application.Pipeline;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Провайдер определения пайплайна из внешнего источника.
/// </summary>
public interface IPipelineDefinitionProvider
{
    /// <summary>
    /// Получить определения пайплайнов.
    /// </summary>
    IReadOnlyList<PipelineDefinition> GetDefinitions();
}
