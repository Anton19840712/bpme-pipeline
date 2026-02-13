using System;
using System.Collections.Generic;
using System.Linq;

namespace Bpme.Application.Pipeline;

/// <summary>
/// Описание пайплайна шагов.
/// </summary>
public sealed record PipelineDefinition(string Tag, IReadOnlyList<PipelineStep> Steps)
{
    /// <summary>
    /// Найти шаг по имени.
    /// </summary>
    public PipelineStep GetStep(string name)
    {
        var step = Steps.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));
        if (step == null)
        {
            throw new InvalidOperationException($"Шаг '{name}' не найден в pipeline.json.");
        }

        return step;
    }

    /// <summary>
    /// Получить предыдущий шаг относительно заданного имени.
    /// </summary>
    public PipelineStep GetPreviousStep(string name)
    {
        var index = -1;
        for (int i = 0; i < Steps.Count; i++)
        {
            if (string.Equals(Steps[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                index = i;
                break;
            }
        }
        if (index <= 0)
        {
            throw new InvalidOperationException($"Для шага '{name}' отсутствует предыдущий шаг.");
        }

        return Steps[index - 1];
    }

    /// <summary>
    /// Получить первый шаг.
    /// </summary>
    public PipelineStep GetFirstStep()
    {
        if (Steps.Count == 0)
        {
            throw new InvalidOperationException("В pipeline.json нет шагов.");
        }

        return Steps[0];
    }
}

/// <summary>
/// Описание шага пайплайна.
/// </summary>
public sealed record PipelineStep(string TopicTag, string Name, IReadOnlyList<PipelineStepParameter>? Parameters = null)
{
    /// <summary>
    /// Получить значение параметра по ключу.
    /// </summary>
    public string? GetParameter(string key)
    {
        if (Parameters == null || Parameters.Count == 0)
        {
            return null;
        }

        foreach (var parameter in Parameters)
        {
            if (string.Equals(parameter.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return parameter.Value;
            }
        }

        return null;
    }
}

/// <summary>
/// Параметр шага пайплайна.
/// </summary>
public sealed record PipelineStepParameter(string Key, string Value);
