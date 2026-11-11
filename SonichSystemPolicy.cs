using SPS.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SPS.Student;

public sealed class SonichSystemPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRequestPolicy();

    private sealed class AdaptiveRequestPolicy : IRequestPolicy
    {
        private readonly ConcurrentQueue<TimeSpan> _recentSuccesses = new();
        private const int MaxSamples = 30;

        private double _timeoutMultiplier = 12.0;
        private readonly object _sync = new();

        private static readonly TimeSpan MinTimeout = TimeSpan.FromMilliseconds(50);
        private static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(20000);
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(200);

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            TimeSpan p95;
            double multiplier;
            lock (_sync)
            {
                if (_recentSuccesses.IsEmpty) return DefaultTimeout;
                var arr = _recentSuccesses.ToArray();
                Array.Sort(arr);
                p95 = arr[(int)(arr.Length * 0.95)];
                multiplier = _timeoutMultiplier;
            }
            return Clamp(TimeSpan.FromTicks((long)(p95.Ticks * multiplier)), MinTimeout, MaxTimeout);
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (observation.Outcome == RequestOutcome.Success)
            {
                _recentSuccesses.Enqueue(observation.Elapsed);
                while (_recentSuccesses.Count > MaxSamples) _recentSuccesses.TryDequeue(out _);
                lock (_sync)
                {
                    if (_recentSuccesses.Count >= 10)
                        _timeoutMultiplier = Math.Max(11.0, _timeoutMultiplier - 1.0);
                }
                return RetryDecision.NoRetry();
            }

            lock (_sync)
            {
                _timeoutMultiplier = Math.Min(20.0, _timeoutMultiplier + 10.0);
            }

            if (observation.Attempt >= 3) return RetryDecision.NoRetry();

            var baseDelayMs = 50.0 * Math.Pow(2, observation.Attempt - 1);
            var jitterMs = random.NextDouble() * 0.2 * baseDelayMs;
            return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(baseDelayMs + jitterMs));
        }

        private static TimeSpan Clamp(TimeSpan v, TimeSpan min, TimeSpan max)
            => v < min ? min : v > max ? max : v;
    }

    //  Состояние
    private readonly ConcurrentDictionary<string, int> _keyPopularity = new();
    private readonly ConcurrentDictionary<long, TimeSpan> _firstSeen = new();
    private TimeSpan _lastCapacityReview = TimeSpan.Zero;

    //  Кэш: чтение
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var key = context.Request.ResourceKey;
        _keyPopularity.AddOrUpdate(key, 1, (_, v) => v + 1);

        if (context.Entry != null && context.Entry.ExpiresAt > context.Now)
        {
            var age = context.Now - context.Entry.SourceObservedAt;
            if (age <= context.Request.MaxDataAge)
                return CacheReadDecision.ServeCached();
        }

        var pending = context.PendingFetches.FirstOrDefault(f => f.Key == key);
        if (pending != null) return CacheReadDecision.JoinFetch(pending.Id);

        return CacheReadDecision.StartFetch();
    }

    //  Кэш: запись
    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var key = context.Value.Key;
        var popularity = _keyPopularity.GetValueOrDefault(key);
        var isSmall = context.Value.SizeBytes <= 4096;

        if (popularity < 2 && !isSmall)
            return CacheWriteDecision.Skip();

        var baseTtlMs = isSmall ? 120_000 : 45_000;
        var ttlMs = Math.Min(baseTtlMs, 20_000 + popularity * 3_000);

        var totalSize = context.Entries.Sum(e => e.SizeBytes) + context.Value.SizeBytes;
        var toEvict = new List<string>();

        if (totalSize > context.CapacityBytes)
        {
            var candidates = context.Entries
                .OrderBy(e => _keyPopularity.GetValueOrDefault(e.Key))
                .ThenByDescending(e => e.SizeBytes)
                .ToList();

            long freed = 0;
            foreach (var entry in candidates)
            {
                if (totalSize - freed <= context.CapacityBytes) break;
                toEvict.Add(entry.Key);
                freed += entry.SizeBytes;
            }
        }
        return CacheWriteDecision.Retain(TimeSpan.FromMilliseconds(ttlMs), toEvict.ToArray());
    }

    //  Маршрутизация API
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;

        var ready = context.Endpoints.Where(e => e.State == InstanceState.Ready).ToList();
        if (ready.Count == 0) return null;

        if (context.Attempt >= 4)
            return EndpointSelectionDecision.Reject();

        var best = ready
            .OrderBy(e => (double)e.OutstandingCalls / Math.Max(1, e.Slots) / Math.Max(0.1, e.Speed))
            .First();

        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        if (context.Endpoint.State == InstanceState.Draining)
            return EndpointDecision.Block();

        var recent = context.RecentResults;
        if (recent.Count >= 3 && !context.Endpoint.ProbeOutstanding)
        {
            var bad = recent.Count(r => r.Outcome != RequestOutcome.Success);
            var successRate = (double)(recent.Count - bad) / recent.Count;

            if (successRate < 0.25)
                return EndpointDecision.Block();
            if (successRate < 0.5)
                return EndpointDecision.Probe();
        }

        return EndpointDecision.Allow();
    }

    //  Назначение заданий
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0)
            return null;

        var candidate = context.QueuedJobs.OrderBy(j => j.QueuedAt).First();

        var waited = context.Now - candidate.QueuedAt;
        var workEstimate = TimeSpan.FromSeconds(candidate.Request.EstimatedWorkSeconds);
        var patienceEstimate = TimeSpan.FromSeconds(25);

        if (waited + workEstimate > patienceEstimate)
            return JobAssignmentDecision.Reject(candidate.Id);

        var worker = context.AvailableWorkers
            .OrderBy(w => (double)w.AssignedJobs / Math.Max(0.1, w.Speed))
            .First();

        return JobAssignmentDecision.Assign(candidate.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var assignedAt = context.Job.AssignedAt;
        if (!assignedAt.HasValue)
            return JobRecoveryDecision.KeepWaiting();

        var workEstimate = TimeSpan.FromSeconds(context.Job.Request.EstimatedWorkSeconds);

        var threshold = TimeSpan.FromTicks((long)(workEstimate.Ticks * 2.5));
        if (threshold < TimeSpan.FromSeconds(20))
            threshold = TimeSpan.FromSeconds(20);

        if (context.Now - assignedAt.Value > threshold)
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromSeconds(1));

        return JobRecoveryDecision.KeepWaiting();
    }

    //  Масштабирование
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        foreach (var inst in context.Instances)
            _firstSeen.TryAdd(inst.Id, context.Now);

        var settings = context.Settings;
        var readyApis = context.Instances
            .Where(i => i.Role == InstanceRole.Api && i.State == InstanceState.Ready).ToList();
        var readyWorkers = context.Instances
            .Where(i => i.Role == InstanceRole.Worker && i.State == InstanceState.Ready).ToList();

        if (context.Now - _lastCapacityReview < TimeSpan.FromSeconds(1))
            return new CapacityPlan(readyApis.Count, readyWorkers.Count);
        _lastCapacityReview = context.Now;

        var apiCu = Math.Max(1, settings.Api.ComputeUnits);
        var workerCu = Math.Max(1, settings.Worker.ComputeUnits);
        var minWorkers = 2;
        var minApis = 1;

        var pending = context.PendingCalls;
        int desiredApis = pending <= 1 ? 1 : (pending <= 4 ? 2 : 3);
        desiredApis = Math.Min(desiredApis, 3);

        var maxApisByCu = Math.Max(minApis, (settings.ComputeUnitLimit - minWorkers * workerCu) / apiCu);
        desiredApis = Math.Clamp(desiredApis, minApis, Math.Min(settings.MaximumApis, maxApisByCu));

        var queued = context.QueuedJobs.Count;
        int desiredWorkers = Math.Max(minWorkers, (queued + 1) / 2);

        var remainingCompute = settings.ComputeUnitLimit - desiredApis * apiCu;
        var maxWorkersByCu = Math.Max(minWorkers, remainingCompute / Math.Max(1, workerCu));
        desiredWorkers = Math.Clamp(desiredWorkers, minWorkers, Math.Min(settings.MaximumWorkers, maxWorkersByCu));

        if (queued == 0 && desiredWorkers > minWorkers)
            desiredWorkers = minWorkers;

        var retire = new List<long>();

        if (readyApis.Count > desiredApis)
        {
            var idle = readyApis
                .Where(i => i.OutstandingCalls == 0 && i.AssignedJobs == 0)
                .Where(i => (context.Now - _firstSeen.GetValueOrDefault(i.Id, TimeSpan.Zero)) > TimeSpan.FromSeconds(10))
                .Take(readyApis.Count - desiredApis);
            retire.AddRange(idle.Select(i => i.Id));
        }

        if (readyWorkers.Count > desiredWorkers)
        {
            var idle = readyWorkers
                .Where(i => i.AssignedJobs == 0 && i.OutstandingCalls == 0)
                .Where(i => (context.Now - _firstSeen.GetValueOrDefault(i.Id, TimeSpan.Zero)) > TimeSpan.FromSeconds(45))
                .Take(readyWorkers.Count - desiredWorkers);
            retire.AddRange(idle.Select(i => i.Id));
        }

        var activeIds = context.Instances.Select(i => i.Id).ToHashSet();
        foreach (var k in _firstSeen.Keys.Where(k => !activeIds.Contains(k)).ToList())
            _firstSeen.TryRemove(k, out _);

        if (_keyPopularity.Count > 50_000)
        {
            var top = _keyPopularity.OrderByDescending(kv => kv.Value).Take(10_000).ToList();
            _keyPopularity.Clear();
            foreach (var kv in top) _keyPopularity[kv.Key] = kv.Value;
        }

        return new CapacityPlan(desiredApis, desiredWorkers, retire);
    }

    //  Наблюдения
    public void Observe(SystemObservation observation)
    {
        switch (observation.Kind)
        {
            case ObservationKind.SimulationStarted:
                if (observation.Settings != null)
                {
                }
                break;

            case ObservationKind.RequestArrived:
                break;

            case ObservationKind.RequestSucceeded:
            case ObservationKind.RequestFailed:
                break;

            case ObservationKind.RpcAttemptStarted:
            case ObservationKind.RpcAttemptCompleted:
            case ObservationKind.ProbeCompleted:
                break;

            case ObservationKind.WorkerHeartbeatReceived:
                break;

            case ObservationKind.JobAssigned:
            case ObservationKind.JobRequeued:
            case ObservationKind.JobCompleted:
                break;

            case ObservationKind.CacheEntryRemoved:
            case ObservationKind.FetchCompleted:
                break;

            case ObservationKind.InstanceStarting:
            case ObservationKind.InstanceReady:
            case ObservationKind.InstanceDraining:
            case ObservationKind.InstanceStopped:
                if (observation.EndpointId.HasValue)
                {
                    if (observation.Kind == ObservationKind.InstanceStopped)
                        _firstSeen.TryRemove(observation.EndpointId.Value, out _);
                }
                break;
        }
    }
}
