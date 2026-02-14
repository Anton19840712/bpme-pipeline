using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Runtime;
using Bpme.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace Bpme.Infrastructure.Storage;

/// <summary>
/// Реальное S3-хранилище (MinIO).
/// </summary>
public sealed class S3ObjectStorage : IObjectStorage
{
    private readonly IAmazonS3 _client;
    private readonly string _bucket;
    private readonly ILogger<S3ObjectStorage> _logger;

    /// <summary>
    /// Создать S3-хранилище.
    /// </summary>
    public S3ObjectStorage(string endpoint, string accessKey, string secretKey, string bucket, bool useSsl, ILogger<S3ObjectStorage> logger)
    {
        _bucket = bucket;
        _logger = logger;
        var config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = true,
            UseHttp = !useSsl
        };

        var creds = new BasicAWSCredentials(accessKey, secretKey);
        _client = new AmazonS3Client(creds, config);
        _logger.LogInformation("S3 client created. Bucket={Bucket}", _bucket);
    }

    /// <summary>
    /// Сохранить объект.
    /// </summary>
    public async Task PutAsync(string key, Stream content, CancellationToken ct)
    {
        await EnsureBucketAsync(ct);

        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            InputStream = content
        };

        await _client.PutObjectAsync(request, ct);
        _logger.LogInformation("S3 put: {Key}", key);
    }

    /// <summary>
    /// Получить объект.
    /// </summary>
    public async Task<Stream> GetAsync(string key, CancellationToken ct)
    {
        var response = await _client.GetObjectAsync(_bucket, key, ct);
        var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms, ct);
        ms.Position = 0;
        response.Dispose();
        _logger.LogInformation("S3 get: {Key}", key);
        return ms;
    }

    /// <summary>
    /// Список объектов.
    /// </summary>
    public async Task<List<string>> ListAsync(string? prefix = null, CancellationToken ct = default)
    {
        var result = new List<string>();
        string? token = null;

        do
        {
            var request = new ListObjectsV2Request
            {
                BucketName = _bucket,
                ContinuationToken = token,
                Prefix = prefix
            };

            var response = await _client.ListObjectsV2Async(request, ct);
            if (response.S3Objects != null)
            {
                result.AddRange(response.S3Objects.Select(o => o.Key));
            }
            token = response.IsTruncated == true ? response.NextContinuationToken : null;
        }
        while (token != null);

        _logger.LogInformation("S3 list: count={Count} prefix={Prefix}", result.Count, prefix ?? "");
        return result;
    }

    /// <summary>
    /// Удалить объект.
    /// </summary>
    public async Task DeleteAsync(string key, CancellationToken ct = default)
    {
        await _client.DeleteObjectAsync(_bucket, key, ct);
        _logger.LogInformation("S3 delete: {Key}", key);
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
            _logger.LogInformation("S3 bucket created: {Bucket}", _bucket);
        }
    }
}
