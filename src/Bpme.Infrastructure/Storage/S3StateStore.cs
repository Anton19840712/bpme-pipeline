using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Runtime;
using Bpme.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Storage;

/// <summary>
/// Хранилище состояния на базе S3 (MinIO).
/// </summary>
public sealed class S3StateStore : IStateStore
{
    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly string _prefix;
    private readonly ILogger<S3StateStore> _logger;

    /// <summary>
    /// Создать S3-хранилище состояния.
    /// </summary>
    public S3StateStore(string endpoint, string accessKey, string secretKey, string bucket, string prefix, bool useSsl, ILogger<S3StateStore> logger)
    {
        _bucket = bucket;
        _prefix = prefix;
        _logger = logger;
        var config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = true,
            UseHttp = !useSsl
        };

        var creds = new BasicAWSCredentials(accessKey, secretKey);
        _client = new AmazonS3Client(creds, config);
        _logger.LogInformation("StateStore initialized. Bucket={Bucket} Prefix={Prefix}", _bucket, _prefix);
    }

    /// <summary>
    /// Проверить, обработан ли файл.
    /// </summary>
    public async Task<bool> IsProcessedAsync(string fileId, CancellationToken ct)
    {
        await EnsureBucketAsync(ct);
        var key = BuildKey(fileId);
        try
        {
            var meta = await _client.GetObjectMetadataAsync(_bucket, key, ct);
            var exists = meta != null;
            _logger.LogInformation("State check: {Key} exists={Exists}", key, exists);
            return exists;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _logger.LogInformation("State check: {Key} exists=false", key);
            return false;
        }
    }

    /// <summary>
    /// Отметить файл как обработанный.
    /// </summary>
    public async Task MarkProcessedAsync(string fileId, CancellationToken ct)
    {
        await EnsureBucketAsync(ct);
        var key = BuildKey(fileId);
        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            ContentBody = "processed"
        };
        await _client.PutObjectAsync(request, ct);
        _logger.LogInformation("State marked: {Key}", key);
    }

    /// <summary>
    /// Убедиться, что bucket существует.
    /// </summary>
    private async Task EnsureBucketAsync(CancellationToken ct)
    {
        var exists = await AmazonS3Util.DoesS3BucketExistV2Async(_client, _bucket);
        if (!exists)
        {
            await _client.PutBucketAsync(_bucket, ct);
            _logger.LogInformation("State bucket created: {Bucket}", _bucket);
        }
    }

    /// <summary>
    /// Построить ключ состояния с учётом префикса.
    /// </summary>
    private string BuildKey(string fileId)
    {
        var cleanPrefix = _prefix?.Trim('/') ?? string.Empty;
        return string.IsNullOrEmpty(cleanPrefix) ? fileId : $"{cleanPrefix}/{fileId}";
    }
}
