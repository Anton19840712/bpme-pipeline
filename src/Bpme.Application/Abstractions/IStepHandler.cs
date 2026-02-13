using Bpme.Domain.Model;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Обработчик события шага.
/// </summary>
public interface IStepHandler
{
    /// <summary>
    /// Тема, на которую подписывается обработчик.
    /// </summary>
    TopicTag Topic { get; }

    /// <summary>
    /// Обработать событие.
    /// </summary>
    Task HandleAsync(PipelineEvent evt, CancellationToken ct);
}