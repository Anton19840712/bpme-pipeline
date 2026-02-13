namespace Bpme.Domain.Model;

/// <summary>
/// Идентификатор файла (например, хэш).
/// </summary>
public sealed record FileId(string Value)
{
    /// <summary>
    /// Создать FileId с валидацией.
    /// </summary>
    public static FileId From(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("FileId не может быть пустым.", nameof(value));
        }

        return new FileId(value.Trim());
    }
}
