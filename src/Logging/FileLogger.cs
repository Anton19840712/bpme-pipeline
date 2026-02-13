using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace Logging;

/// <summary>
/// Логгер в файл.
/// </summary>
public sealed class FileLogger : ILogger
{
    private static readonly object LockObj = new();
    private readonly string _path;
    private readonly LogLevel _minLevel;
    private readonly string _category;

    /// <summary>
    /// Создать логгер.
    /// </summary>
    public FileLogger(string path, LogLevel minLevel, string category)
    {
        _path = path;
        _minLevel = minLevel;
        _category = category;
    }

    /// <summary>
    /// Начать scope.
    /// </summary>
    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    /// <summary>
    /// Проверка уровня.
    /// </summary>
    public bool IsEnabled(LogLevel logLevel) => logLevel >= _minLevel;

    /// <summary>
    /// Записать лог.
    /// </summary>
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var message = formatter(state, exception);
        var line = $"{DateTimeOffset.UtcNow:O} [{logLevel}] {_category} {message}";
        if (exception != null)
        {
            line += $" | {exception.GetType().Name}: {exception.Message}";
        }

        WriteLineShared(_path, line);
    }

    /// <summary>
    /// Записать строку с общим блокировочным доступом.
    /// </summary>
    private static void WriteLineShared(string path, string line)
    {
        lock (LockObj)
        {
            using var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            using var sw = new StreamWriter(fs);
            sw.WriteLine(line);
        }
    }

    /// <summary>
    /// Пустой scope.
    /// </summary>
    private sealed class NullScope : IDisposable
    {
        /// <summary>
        /// Единственный экземпляр.
        /// </summary>
        public static readonly NullScope Instance = new();

        /// <summary>
        /// Освободить ресурсы.
        /// </summary>
        public void Dispose()
        {
        }
    }
}
