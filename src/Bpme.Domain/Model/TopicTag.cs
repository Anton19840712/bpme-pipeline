namespace Bpme.Domain.Model;

/// <summary>
/// Тег темы для маршрутизации событий.
/// </summary>
public sealed record TopicTag(string Value)
{
    /// <summary>
    /// Создать TopicTag с валидацией.
    /// </summary>
    public static TopicTag From(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("TopicTag не может быть пустым.", nameof(value));
        }

        return new TopicTag(value.Trim());
    }
}