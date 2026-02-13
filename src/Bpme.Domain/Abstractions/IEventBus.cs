using Bpme.Domain.Model;

namespace Bpme.Domain.Abstractions;

/// <summary>
/// Шина событий.
/// </summary>
public interface IEventBus
{
    /// <summary>
    /// Подписаться на тему.
    /// </summary>
    void Subscribe(TopicTag topic, Func<PipelineEvent, CancellationToken, Task> handler);

    /// <summary>
    /// Опубликовать событие.
    /// </summary>
    Task PublishAsync(PipelineEvent evt, CancellationToken ct = default);
}