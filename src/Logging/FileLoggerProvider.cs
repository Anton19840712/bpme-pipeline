using Microsoft.Extensions.Logging;

namespace Logging;

/// <summary>
/// Провайдер файлового логгера.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly string _filePath;
    private readonly LogLevel _minLevel;

    /// <summary>
    /// Создать провайдер.
    /// </summary>
    public FileLoggerProvider(string filePath, LogLevel minLevel)
    {
        _filePath = filePath;
        _minLevel = minLevel;
    }

    /// <summary>
    /// Создать логгер.
    /// </summary>
    public ILogger CreateLogger(string categoryName)
    {
        return new FileLogger(_filePath, _minLevel, categoryName);
    }

    /// <summary>
    /// Освободить ресурсы.
    /// </summary>
    public void Dispose()
    {
    }
}
