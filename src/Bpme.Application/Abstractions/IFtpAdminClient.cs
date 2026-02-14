namespace Bpme.Application.Abstractions;

public interface IFtpAdminClient
{
    List<string> List(string? searchPathOverride = null);
    void Upload(string remoteName, Stream content, string? searchPathOverride = null);
    Stream Download(string remoteName);
    void Delete(string remoteName);
    int Clear();
}

