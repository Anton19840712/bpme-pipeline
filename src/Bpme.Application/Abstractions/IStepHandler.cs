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

/// <summary>
/// Контракт шага с несколькими темами входа.
/// </summary>
public interface IMultiTopicStepHandler
{
    /// <summary>
    /// Темы, на которые подписан шаг.
    /// </summary>
    IReadOnlyList<TopicTag> Topics { get; }
}

/// <summary>
/// Контракт для имени шага.
/// </summary>
public interface IStepNameProvider
{
    /// <summary>
    /// Имя шага, которое используется в pipeline JSON.
    /// </summary>
    string StepName { get; }
}
