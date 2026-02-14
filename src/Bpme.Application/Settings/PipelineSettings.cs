namespace Bpme.Application.Settings;

/// <summary>
/// Корневые настройки конвейера.
/// </summary>
public sealed record PipelineSettings(
    SinkSettings Sink,
    EventPayloadSettings EventPayload,
    FileRetentionSettings FileRetention,
    FtpDetectionSettings FtpDetection,
    FtpConnectionSettings FtpConnection,
    FtpClientSettings FtpClient,
    S3Settings S3,
    RabbitMqSettings RabbitMq,
    PipelineTopicsSettings PipelineTopics,
    StoragePathsSettings StoragePaths,
    PipelineDefinitionSettings PipelineDefinition,
    AdminApiSettings AdminApi,
    LoggingSettings Logging,
    DevSettings? Dev = null,
    DeploySettings? Deploy = null
);

/// <summary>
/// Настройки логирования как финального шага.
/// </summary>
public sealed record SinkSettings(
    string Type,
    bool Pretty,
    int MaxItems,
    string Level,
    string Format,
    string Output,
    bool LogJson
);

/// <summary>
/// Настройки передачи payload в событиях.
/// </summary>
public sealed record EventPayloadSettings(
    string Mode,
    bool IncludeDataInEvent
);

/// <summary>
/// Политика хранения файлов.
/// </summary>
public sealed record FileRetentionSettings(
    bool KeepRaw,
    int RawTtlDays,
    int ParsedTtlDays
);

/// <summary>
/// Настройки детекта FTP.
/// </summary>
public sealed record FtpDetectionSettings(
    string Strategy,
    int PeriodInSeconds,
    string SearchPath,
    string Mask,
    int NotOlderThanSeconds,
    int StableForSeconds,
    StateStoreSettings StateStore,
    DeduplicationSettings Deduplication,
    TriggerMode TriggerMode = TriggerMode.Both
);

/// <summary>
/// Режимы запуска процессов.
/// </summary>
public enum TriggerMode
{
    /// <summary>
    /// Разрешен только фоновый запуск.
    /// </summary>
    Background = 0,

    /// <summary>
    /// Разрешен только ручной запуск.
    /// </summary>
    Manual = 1,

    /// <summary>
    /// Разрешены оба режима.
    /// </summary>
    Both = 2
}

/// <summary>
/// Настройки подключения к FTP.
/// </summary>
public sealed record FtpConnectionSettings(
    string Host,
    int Port,
    string User,
    string Password,
    bool UseSsl
);

/// <summary>
/// Настройки FTP клиента.
/// </summary>
public sealed record FtpClientSettings(
    string EncryptionMode,
    string DataConnectionType,
    string InternetProtocol,
    int ReadTimeoutMs,
    int ConnectTimeoutMs,
    bool SocketKeepAlive
);

/// <summary>
/// Настройки S3 (MinIO).
/// </summary>
public sealed record S3Settings(
    string Endpoint,
    string AccessKey,
    string SecretKey,
    string Bucket,
    bool UseSsl
);

/// <summary>
/// Настройки RabbitMQ.
/// </summary>
public sealed record RabbitMqSettings(
    string Host,
    int Port,
    string User,
    string Password,
    string VirtualHost,
    string Exchange,
    string QueuePrefix
);

/// <summary>
/// Настройки AdminApi.
/// </summary>
public sealed record AdminApiSettings(
    long MaxUploadBytes
);

/// <summary>
/// Настройки логирования.
/// </summary>
public sealed record LoggingSettings(
    string Directory,
    string? FilePath = null
);

/// <summary>
/// Настройки хранилища состояния.
/// </summary>
public sealed record StateStoreSettings(
    string Type,
    string Bucket,
    string Prefix
);

/// <summary>
/// Политика дедупликации.
/// </summary>
public sealed record DeduplicationSettings(
    string Mode
);

/// <summary>
/// Темы событий конвейера.
/// </summary>
public sealed record PipelineTopicsSettings(
    string TriggerTopic,
    string ScanTopic,
    string ParsingTopic
);

/// <summary>
/// Настройки определения пайплайна через JSON.
/// </summary>
public sealed record PipelineDefinitionSettings(
    string? FileName = null,
    IReadOnlyList<string>? FileNames = null
);

/// <summary>
/// Префиксы хранения в S3.
/// </summary>
public sealed record StoragePathsSettings(
    string RawPrefix,
    string ParsedPrefix
);

/// <summary>
/// Настройки dev окружения.
/// </summary>
public sealed record DevSettings(DevDockerSettings Docker);

/// <summary>
/// Настройки dev docker.
/// </summary>
public sealed record DevDockerSettings(
    string RabbitmqUrl,
    string S3Endpoint,
    string S3AccessKey,
    string S3SecretKey,
    string S3Bucket,
    string FtpHost,
    string FtpUser,
    string FtpPassword
);

/// <summary>
/// Настройки деплоя.
/// </summary>
public sealed record DeploySettings(SwarmSettings Swarm);

/// <summary>
/// Настройки Docker Swarm.
/// </summary>
public sealed record SwarmSettings(
    bool UseEnv,
    string EnvPrefix,
    string ConfigStrategy
);
