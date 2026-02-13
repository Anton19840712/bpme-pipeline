using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Microsoft.Extensions.Configuration;

namespace Bpme.Infrastructure.Config;

/// <summary>
/// Провайдер настроек из appsettings.json.
/// </summary>
public sealed class AppSettingsProvider : ISettingsProvider
{
    private readonly IConfiguration _configuration;

    /// <summary>
    /// Создать провайдер.
    /// </summary>
    public AppSettingsProvider(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    /// <summary>
    /// Загрузить настройки.
    /// </summary>
    public PipelineSettings Load()
    {
        var settings = _configuration.Get<PipelineSettings>();
        if (settings == null)
        {
            throw new InvalidOperationException("Не удалось загрузить настройки из appsettings.json.");
        }

        return settings;
    }
}
