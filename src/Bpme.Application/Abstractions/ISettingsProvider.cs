using Bpme.Application.Settings;

namespace Bpme.Application.Abstractions;

/// <summary>
/// Провайдер настроек приложения.
/// </summary>
public interface ISettingsProvider
{
    /// <summary>
    /// Загрузить настройки.
    /// </summary>
    PipelineSettings Load();
}
