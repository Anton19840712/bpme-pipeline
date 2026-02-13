using System.Text.Json;
using Bpme.Domain.Abstractions;
using Bpme.Domain.Model;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Bpme.Infrastructure.Bus;

/// <summary>
/// Реальная шина RabbitMQ.
/// </summary>
public sealed class RabbitMqEventBus : IEventBus, IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchange;
    private readonly string _queuePrefix;
    private readonly ILogger<RabbitMqEventBus> _logger;

    /// <summary>
    /// Создать шину RabbitMQ.
    /// </summary>
    public RabbitMqEventBus(
        string host,
        int port,
        string user,
        string password,
        string vhost,
        string exchange,
        string queuePrefix,
        ILogger<RabbitMqEventBus> logger)
    {
        _exchange = exchange;
        _queuePrefix = queuePrefix;
        _logger = logger;
        var factory = new ConnectionFactory
        {
            HostName = host,
            Port = port,
            UserName = user,
            Password = password,
            VirtualHost = vhost,
            DispatchConsumersAsync = true
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(_exchange, ExchangeType.Direct, durable: true, autoDelete: false);
        _logger.LogInformation("RabbitMQ connected. Exchange={Exchange} QueuePrefix={QueuePrefix}", _exchange, _queuePrefix);
    }

    /// <summary>
    /// Подписаться на тему.
    /// </summary>
    public void Subscribe(TopicTag topic, Func<PipelineEvent, CancellationToken, Task> handler)
    {
        var queue = $"{_queuePrefix}.{topic.Value}";
        _channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
        _channel.QueueBind(queue, _exchange, routingKey: topic.Value);

        _logger.LogInformation("Subscribed to {Topic} with queue {Queue}", topic.Value, queue);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += async (_, ea) =>
        {
            try
            {
                var json = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
                var evt = JsonSerializer.Deserialize<PipelineEvent>(json);
                if (evt != null)
                {
                    using (_logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId }))
                    {
                        _logger.LogInformation("Event received: {RoutingKey} size={Size}", ea.RoutingKey, ea.Body.Length);
                        await handler(evt, CancellationToken.None);
                    }
                }
                else
                {
                    _logger.LogWarning("Event deserialization failed. RoutingKey={RoutingKey}", ea.RoutingKey);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Handler failed for routingKey={RoutingKey}", ea.RoutingKey);
            }
            finally
            {
                _channel.BasicAck(ea.DeliveryTag, multiple: false);
            }
        };

        _channel.BasicConsume(queue, autoAck: false, consumer: consumer);
    }

    /// <summary>
    /// Опубликовать событие.
    /// </summary>
    public Task PublishAsync(PipelineEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(evt);
        var body = System.Text.Encoding.UTF8.GetBytes(json);
        var props = _channel.CreateBasicProperties();
        props.Persistent = true;

        _channel.BasicPublish(_exchange, routingKey: evt.Topic.Value, basicProperties: props, body: body);
        using (_logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = evt.CorrelationId }))
        {
            _logger.LogInformation("Event published: {Topic} payloadKeys={Keys}", evt.Topic.Value, string.Join(",", evt.Payload.Keys));
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Освободить ресурсы.
    /// </summary>
    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
        _logger.LogInformation("RabbitMQ disposed");
    }
}