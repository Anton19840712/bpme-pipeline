namespace Bpme.Application.Abstractions;

/// <summary>
/// Хранилище состояния обработки файлов.
/// </summary>
public interface IStateStore
{
    /// <summary>
    /// Проверить, обработан ли файл.
    /// </summary>
    Task<bool> IsProcessedAsync(string fileId, CancellationToken ct);

    /// <summary>
    /// Отметить файл как обработанный.
    /// </summary>
    Task MarkProcessedAsync(string fileId, CancellationToken ct);
}