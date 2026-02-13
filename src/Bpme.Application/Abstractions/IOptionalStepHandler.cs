namespace Bpme.Application.Abstractions;

/// <summary>
/// Обработчик шага, который можно отключить через конфигурацию.
/// </summary>
public interface IOptionalStepHandler
{
    /// <summary>
    /// Признак включения обработчика.
    /// </summary>
    bool Enabled { get; }
}
