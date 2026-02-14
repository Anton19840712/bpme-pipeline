using Bpme.Application.Pipeline;
using Bpme.Application.Settings;
using Bpme.Application.Abstractions;

namespace Bpme.AdminApi;

public sealed record FtpUploadResult(string FileName, string[] Processes);

public sealed class FtpIngressService
{
    private readonly PipelineSettings _settings;
    private readonly IPipelineDefinitionRegistry _registry;
    private readonly IProcessTriggerService _triggerService;
    private readonly IFtpAdminClient _ftp;
    private readonly ILogger<FtpIngressService> _logger;

    public FtpIngressService(
        PipelineSettings settings,
        IPipelineDefinitionRegistry registry,
        IProcessTriggerService triggerService,
        IFtpAdminClient ftp,
        ILogger<FtpIngressService> logger)
    {
        _settings = settings;
        _registry = registry;
        _triggerService = triggerService;
        _ftp = ftp;
        _logger = logger;
    }

    public async Task<FtpUploadResult> UploadAndTriggerAsync(
        string fileName,
        Stream content,
        string? processTag,
        CancellationToken ct)
    {
        var uploadPath = _settings.FtpDetection.SearchPath;
        var definitions = ResolveTargetDefinitions(processTag);
        var ftpDefinition = definitions.FirstOrDefault();
        if (ftpDefinition != null)
        {
            var step = _registry.GetStep(ftpDefinition, "ftpScan");
            uploadPath = step.GetParameter("searchPath") ?? uploadPath;
        }

        _logger.LogInformation("FTP upload start. name={Name} size={Size}", fileName, content.Length);
        _logger.LogInformation("FTP upload target directory. path={Path}", uploadPath ?? "/");

        _ftp.Upload(fileName, content, uploadPath);
        var files = _ftp.List(uploadPath);
        _logger.LogInformation(
            "FTP files after upload. path={Path} count={Count} names={Names}",
            uploadPath ?? "/",
            files.Count,
            string.Join(", ", files));
        _logger.LogInformation("FTP upload done. name={Name}", fileName);

        foreach (var definition in definitions)
        {
            var payload = new Dictionary<string, string>
            {
                ["fileName"] = fileName
            };
            await _triggerService.PublishTriggerAsync(definition, "manualTrigger", "runtime", payload, ct);
        }

        return new FtpUploadResult(fileName, definitions.Select(x => x.Tag).ToArray());
    }

    private List<PipelineDefinition> ResolveTargetDefinitions(string? processTag)
    {
        if (!string.IsNullOrWhiteSpace(processTag))
        {
            return new List<PipelineDefinition> { _registry.GetByTag(processTag) };
        }

        var definitions = _registry.GetAll()
            .Where(d => d.Steps.Any(s => string.Equals(s.Name, "ftpScan", StringComparison.OrdinalIgnoreCase)))
            .ToList();

        if (definitions.Count == 0)
        {
            _logger.LogWarning("FTP upload trigger skipped. No FTP pipelines found.");
        }

        return definitions;
    }
}
