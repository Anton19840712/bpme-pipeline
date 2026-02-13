using Bpme.Domain.Model;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Контракт обработчика шага.
/// </summary>
public interface IStepHandler
{
    /// <summary>
    /// Тема, на которую подписан шаг.
    /// </summary>
    TopicTag Topic { get; }

    /// <summary>
    /// Обработать входящее событие.
    /// </summary>
    Task HandleAsync(PipelineEvent evt, CancellationToken ct);
}
