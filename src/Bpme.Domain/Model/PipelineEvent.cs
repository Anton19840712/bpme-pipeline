using System.Collections.Generic;

namespace Bpme.Domain.Model;

/// <summary>
/// Событие конвейера.
/// </summary>
public sealed record PipelineEvent(
    TopicTag Topic,
    string CorrelationId,
    IReadOnlyDictionary<string, string> Payload
);