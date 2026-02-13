namespace Bpme.Application.Abstractions;

/// <summary>
/// Маркер для опциональных шагов.
/// </summary>
public interface IOptionalStepHandler
{
    /// <summary>
    /// Включён ли шаг.
    /// </summary>
    bool Enabled { get; }
}
