using System.Net;
using Bpme.Application.Abstractions;
using Bpme.Application.Settings;
using FluentFTP;

namespace Bpme.Infrastructure.Admin;

public sealed class FtpAdminClient : IFtpAdminClient
{
    private readonly PipelineSettings _settings;

    public FtpAdminClient(PipelineSettings settings)
    {
        _settings = settings;
    }

    public List<string> List(string? searchPathOverride = null)
    {
        using var client = CreateFtpClient();
        client.Connect();
        var basePath = NormalizeBasePath(searchPathOverride);
        client.SetWorkingDirectory(basePath);
        var items = client.GetListing();
        return items.Where(i => i.Type == FtpObjectType.File).Select(i => i.Name).ToList();
    }

    public void Upload(string remoteName, Stream content, string? searchPathOverride = null)
    {
        using var client = CreateFtpClient();
        client.Connect();
        var basePath = NormalizeBasePath(searchPathOverride);
        if (basePath != "/")
        {
            try
            {
                client.SetWorkingDirectory(basePath);
            }
            catch (FluentFTP.Exceptions.FtpCommandException)
            {
                client.CreateDirectory(basePath, true);
                client.SetWorkingDirectory(basePath);
            }
        }

        var fileName = Path.GetFileName(remoteName);
        content.Position = 0;
        var status = client.UploadStream(content, fileName, FtpRemoteExists.Overwrite, true);
        if (status != FtpStatus.Success)
        {
            throw new InvalidOperationException($"FTP upload failed for {fileName}. Status={status}");
        }
    }

    public Stream Download(string remoteName)
    {
        var ms = new MemoryStream();
        using var client = CreateFtpClient();
        client.Connect();
        var basePath = NormalizeBasePath(null);
        client.SetWorkingDirectory(basePath);
        var fileName = Path.GetFileName(remoteName);
        client.DownloadStream(ms, fileName);
        ms.Position = 0;
        return ms;
    }

    public void Delete(string remoteName)
    {
        using var client = CreateFtpClient();
        client.Connect();
        var basePath = NormalizeBasePath(null);
        client.SetWorkingDirectory(basePath);
        var fileName = Path.GetFileName(remoteName);
        client.DeleteFile(fileName);
    }

    public int Clear()
    {
        using var client = CreateFtpClient();
        client.Connect();
        var basePath = NormalizeBasePath(null);
        client.SetWorkingDirectory(basePath);
        var items = client.GetListing();
        var files = items.Where(i => i.Type == FtpObjectType.File).ToList();
        foreach (var file in files)
        {
            client.DeleteFile(file.Name);
        }

        return files.Count;
    }

    private FtpClient CreateFtpClient()
    {
        var ftp = _settings.FtpConnection;
        var clientSettings = _settings.FtpClient;
        var client = new FtpClient(ftp.Host)
        {
            Port = ftp.Port,
            Credentials = new NetworkCredential(ftp.User, ftp.Password)
        };

        client.Config.EncryptionMode = Enum.TryParse<FtpEncryptionMode>(clientSettings.EncryptionMode, true, out var enc)
            ? enc
            : FtpEncryptionMode.None;
        client.Config.DataConnectionType = Enum.TryParse<FtpDataConnectionType>(clientSettings.DataConnectionType, true, out var dataType)
            ? dataType
            : FtpDataConnectionType.PASV;
        client.Config.InternetProtocolVersions = Enum.TryParse<FtpIpVersion>(clientSettings.InternetProtocol, true, out var ipVer)
            ? ipVer
            : FtpIpVersion.IPv4;
        client.Config.ReadTimeout = clientSettings.ReadTimeoutMs;
        client.Config.ConnectTimeout = clientSettings.ConnectTimeoutMs;
        client.Config.SocketKeepAlive = clientSettings.SocketKeepAlive;

        if (ftp.UseSsl)
        {
            client.Config.EncryptionMode = FtpEncryptionMode.Explicit;
            client.Config.ValidateAnyCertificate = true;
        }

        return client;
    }

    private string NormalizeBasePath(string? searchPathOverride)
    {
        var basePath = searchPathOverride?.Trim() ?? _settings.FtpDetection.SearchPath?.Trim() ?? string.Empty;
        basePath = basePath.Replace('\\', '/');
        if (string.IsNullOrWhiteSpace(basePath) || basePath == "/")
        {
            return "/";
        }
        if (!basePath.StartsWith('/'))
        {
            basePath = "/" + basePath;
        }
        if (basePath.EndsWith('/'))
        {
            basePath = basePath.TrimEnd('/');
        }
        return basePath;
    }
}

