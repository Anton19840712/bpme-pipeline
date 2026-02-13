using Bpme.Application.Pipeline;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Провайдер определения пайплайна из внешнего источника.
/// </summary>
public interface IPipelineDefinitionProvider
{
    /// <summary>
    /// Получить определение пайплайна.
    /// </summary>
    PipelineDefinition GetDefinition();
}
