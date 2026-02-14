using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using Bpme.Domain.Abstractions;
using Bpme.Infrastructure.Admin;
using Bpme.Infrastructure.Bus;
using Bpme.Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddBpmeInfrastructure(this IServiceCollection services, PipelineSettings settings)
    {
        services.AddSingleton<S3ObjectStorage>(sp =>
            new S3ObjectStorage(
                settings.S3.Endpoint,
                settings.S3.AccessKey,
                settings.S3.SecretKey,
                settings.S3.Bucket,
                settings.S3.UseSsl,
                sp.GetRequiredService<ILogger<S3ObjectStorage>>()));

        services.AddSingleton<IObjectStorage>(sp => sp.GetRequiredService<S3ObjectStorage>());

        services.AddSingleton<IStateStore>(sp =>
        {
            var state = settings.FtpDetection.StateStore;
            return new S3StateStore(
                settings.S3.Endpoint,
                settings.S3.AccessKey,
                settings.S3.SecretKey,
                state.Bucket,
                state.Prefix,
                settings.S3.UseSsl,
                sp.GetRequiredService<ILogger<S3StateStore>>());
        });

        services.AddSingleton<RabbitMqEventBus>(sp =>
            new RabbitMqEventBus(
                settings.RabbitMq.Host,
                settings.RabbitMq.Port,
                settings.RabbitMq.User,
                settings.RabbitMq.Password,
                settings.RabbitMq.VirtualHost,
                settings.RabbitMq.Exchange,
                settings.RabbitMq.QueuePrefix,
                sp.GetRequiredService<ILogger<RabbitMqEventBus>>()));

        services.AddSingleton<IEventBus>(sp => sp.GetRequiredService<RabbitMqEventBus>());
        services.AddSingleton<IFtpAdminClient, FtpAdminClient>();
        services.AddSingleton<IRabbitMqManagementClient, RabbitMqManagementClient>();

        return services;
    }
}

