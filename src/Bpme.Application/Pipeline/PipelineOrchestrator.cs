using Bpme.Application.Abstractions;
using Bpme.Domain.Abstractions;

namespace Bpme.Application.Pipeline;

/// <summary>
/// Оркестратор конвейера: подключает обработчики к шине.
/// </summary>
public sealed class PipelineOrchestrator
{
    private readonly IEventBus _eventBus;
    private readonly IEnumerable<IStepHandler> _handlers;

    /// <summary>
    /// Создать оркестратор.
    /// </summary>
    public PipelineOrchestrator(IEventBus eventBus, IEnumerable<IStepHandler> handlers)
    {
        _eventBus = eventBus;
        _handlers = handlers;
    }

    /// <summary>
    /// Зарегистрировать обработчики в шине.
    /// </summary>
    public void RegisterHandlers()
    {
        foreach (var handler in _handlers)
        {
            if (handler is IOptionalStepHandler optional && !optional.Enabled)
            {
                continue;
            }

            _eventBus.Subscribe(handler.Topic, handler.HandleAsync);
        }
    }
}
