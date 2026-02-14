using System.Collections.Concurrent;

namespace Bpme.AdminApi;

/// <summary>
/// Хранилище итераций запуска процессов.
/// </summary>
public sealed class ProcessIterationStore
{
    private readonly ConcurrentDictionary<string, int> _iterations = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Получить следующую итерацию для процесса.
    /// </summary>
    public int Next(string processTag)
    {
        return _iterations.AddOrUpdate(processTag, 1, (_, current) => current + 1);
    }
}

