namespace Bpme.Application.Abstractions;

/// <summary>
/// Хранилище объектов (S3 и аналоги).
/// </summary>
public interface IObjectStorage
{
    /// <summary>
    /// Сохранить объект по ключу.
    /// </summary>
    Task PutAsync(string key, Stream content, CancellationToken ct);

    /// <summary>
    /// Получить объект по ключу.
    /// </summary>
    Task<Stream> GetAsync(string key, CancellationToken ct);

    /// <summary>
    /// Получить список объектов по префиксу.
    /// </summary>
    Task<List<string>> ListAsync(string? prefix = null, CancellationToken ct = default);

    /// <summary>
    /// Удалить объект по ключу.
    /// </summary>
    Task DeleteAsync(string key, CancellationToken ct = default);
}

