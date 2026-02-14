using System.Text.Json;

namespace Bpme.Application.Abstractions;

public interface IRabbitMqManagementClient
{
    Task<List<JsonElement>> GetQueuesAsync(CancellationToken ct = default);
    Task<string> PeekAsync(string queue, int count, CancellationToken ct = default);
}

