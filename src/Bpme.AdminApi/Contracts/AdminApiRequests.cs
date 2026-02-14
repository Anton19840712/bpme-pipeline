namespace Bpme.AdminApi;

/// <summary>
/// Запрос на просмотр сообщений в очереди.
/// </summary>
public sealed record RabbitPeekRequest(string Queue, int Count);

/// <summary>
/// Запрос на публикацию события.
/// </summary>
public sealed record RabbitPublishRequest(string Topic, Dictionary<string, string>? Payload);

/// <summary>
/// Запрос на загрузку файла в FTP.
/// </summary>
public sealed record FtpUploadRequest(IFormFile File);

/// <summary>
/// Запрос на загрузку файла в S3.
/// </summary>
public sealed record S3UploadRequest(IFormFile File, string Key);

