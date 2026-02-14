using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;

namespace Bpme.Infrastructure.Admin;

public sealed class RabbitMqManagementClient : IRabbitMqManagementClient
{
    private readonly PipelineSettings _settings;
    private readonly IHttpClientFactory _httpClientFactory;

    public RabbitMqManagementClient(PipelineSettings settings, IHttpClientFactory httpClientFactory)
    {
        _settings = settings;
        _httpClientFactory = httpClientFactory;
    }

    public async Task<List<JsonElement>> GetQueuesAsync(CancellationToken ct = default)
    {
        var json = await GetAsync("/api/queues", ct);
        var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateArray().ToList();
    }

    public async Task<string> PeekAsync(string queue, int count, CancellationToken ct = default)
    {
        var vhost = _settings.RabbitMq.VirtualHost == "/" ? "%2F" : _settings.RabbitMq.VirtualHost;
        var path = $"/api/queues/{vhost}/{queue}/get";
        var payload = new
        {
            count,
            ackmode = "ack_requeue_true",
            encoding = "auto",
            truncate = 20000
        };

        return await PostAsync(path, payload, ct);
    }

    private async Task<string> GetAsync(string path, CancellationToken ct)
    {
        var host = _settings.RabbitMq.Host == "127.0.0.1" ? "127.0.0.1" : _settings.RabbitMq.Host;
        var url = $"http://{host}:15672{path}";

        using var client = _httpClientFactory.CreateClient();
        var auth = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_settings.RabbitMq.User}:{_settings.RabbitMq.Password}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
        return await client.GetStringAsync(url, ct);
    }

    private async Task<string> PostAsync(string path, object payload, CancellationToken ct)
    {
        var host = _settings.RabbitMq.Host == "127.0.0.1" ? "127.0.0.1" : _settings.RabbitMq.Host;
        var url = $"http://{host}:15672{path}";

        using var client = _httpClientFactory.CreateClient();
        var auth = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_settings.RabbitMq.User}:{_settings.RabbitMq.Password}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
        var jsonPayload = JsonSerializer.Serialize(payload);
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await client.PostAsync(url, content, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct);
    }
}

