using SPS.Abstractions;

namespace SPS.Student;

public sealed class SonichSystemPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new SonichRequestPolicy();

    private sealed class SonichRequestPolicy : IRequestPolicy
    {
        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            return TimeSpan.FromSeconds(60);
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (observation.Outcome == RequestOutcome.Timeout ||
                observation.Outcome == RequestOutcome.Unavailable)
            {
                return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(100));
            }
            return RetryDecision.NoRetry();
        }
    }

    private readonly Dictionary<string, int> _keyPopularity = new();
    private readonly Dictionary<long, TimeSpan> _firstSeen = new();
    private TimeSpan _lastCapacityReview = TimeSpan.Zero;

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var key = context.Request.ResourceKey;
        _keyPopularity[key] = _keyPopularity.GetValueOrDefault(key) + 1;

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

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var key = context.Value.Key;
        var popularity = _keyPopularity.GetValueOrDefault(key);
        var isSmall = context.Value.SizeBytes <= 4096;

        if (popularity < 1 && !isSmall)
            return CacheWriteDecision.Skip();

        var baseTtl = isSmall ? 120_000 : 45_000;
        var ttlMs = Math.Min(baseTtl, 30_000 + popularity * 5_000);

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

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;

        var ready = context.Endpoints.Where(e => e.State == InstanceState.Ready).ToList();
        if (ready.Count == 0) return null;

        var best = ready
            .OrderBy(e => (double)e.OutstandingCalls / Math.Max(1, e.Slots))
            .First();

        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        if (context.Endpoint.State == InstanceState.Draining) return EndpointDecision.Block();
        return EndpointDecision.Allow();
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0)
            return null;

        var candidate = context.QueuedJobs
            .OrderBy(j => j.QueuedAt)
            .FirstOrDefault();

        if (candidate == null) return null;

        var waited = context.Now - candidate.QueuedAt;

        if (waited > TimeSpan.FromSeconds(25))
        {
            return JobAssignmentDecision.Reject(candidate.Id);
        }

        var worker = context.AvailableWorkers.OrderBy(w => w.AssignedJobs).First();
        return JobAssignmentDecision.Assign(candidate.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var now = context.Now;

        if (context.Job.AssignedAt.HasValue &&
            (now - context.Job.AssignedAt.Value) > TimeSpan.FromSeconds(90))
        {
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromSeconds(1));
        }

        return JobRecoveryDecision.KeepWaiting();
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        foreach (var inst in context.Instances)
            if (!_firstSeen.ContainsKey(inst.Id)) _firstSeen[inst.Id] = context.Now;

        var settings = context.Settings;
        var readyApisNow = context.Instances.Count(i => i.Role == InstanceRole.Api && i.State == InstanceState.Ready);
        var readyWorkersNow = context.Instances.Count(i => i.Role == InstanceRole.Worker && i.State == InstanceState.Ready);

        if (context.Now - _lastCapacityReview < TimeSpan.FromSeconds(1))
            return new CapacityPlan(readyApisNow, readyWorkersNow);

        _lastCapacityReview = context.Now;

        int queuedJobs = context.QueuedJobs.Count;

        int desiredApis = queuedJobs > 5 ? 2 : 1;

        var apiCu = Math.Max(1, settings.Api.ComputeUnits);
        var workerCu = Math.Max(1, settings.Worker.ComputeUnits);
        var minWorkers = 2;

        var maxApisByCu = Math.Max(1, (settings.ComputeUnitLimit - minWorkers * workerCu) / apiCu);
        desiredApis = Math.Clamp(desiredApis, 1, Math.Min(settings.MaximumApis, maxApisByCu));

        var remainingCompute = settings.ComputeUnitLimit - desiredApis * apiCu;
        var maxWorkers = remainingCompute / Math.Max(1, workerCu);
        var desiredWorkers = Math.Clamp(maxWorkers, minWorkers, settings.MaximumWorkers);

        var retire = new List<long>();

        if (readyApisNow > desiredApis)
        {
            var idle = context.Instances
                .Where(i => i.Role == InstanceRole.Api && i.State == InstanceState.Ready && i.OutstandingCalls == 0)
                .Where(i => (context.Now - _firstSeen.GetValueOrDefault(i.Id, TimeSpan.Zero)) > TimeSpan.FromSeconds(10))
                .Take(readyApisNow - desiredApis);
            retire.AddRange(idle.Select(i => i.Id));
        }

        if (readyWorkersNow > desiredWorkers)
        {
            var idle = context.Instances
                .Where(i => i.Role == InstanceRole.Worker && i.State == InstanceState.Ready && i.AssignedJobs == 0)
                .Where(i => (context.Now - _firstSeen.GetValueOrDefault(i.Id, TimeSpan.Zero)) > TimeSpan.FromSeconds(60))
                .Take(readyWorkersNow - desiredWorkers);
            retire.AddRange(idle.Select(i => i.Id));
        }

        var activeIds = context.Instances.Select(i => i.Id).ToHashSet();
        foreach (var k in _firstSeen.Keys.Where(k => !activeIds.Contains(k)).ToList())
            _firstSeen.Remove(k);

        return new CapacityPlan(desiredApis, desiredWorkers, retire);
    }

    public void Observe(SystemObservation observation)
    {
    }
}
