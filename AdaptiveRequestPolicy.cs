using RRC.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RRC.Student

{
    public sealed class AdaptiveRequestPolicy : IRequestPolicy
    {

        private readonly Queue<TimeSpan> _recentSuccesses = new();
        private const int MaxSamples = 30;
        private double _timeoutMultiplier = 1.2;
        private int _consecutiveTimeouts = 0;

        private static readonly TimeSpan MinTimeout = TimeSpan.FromMilliseconds(50);
        private static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(2000);
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(200);

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            if (_recentSuccesses.Count == 0)
                return DefaultTimeout;

            var sorted = _recentSuccesses.ToArray();
            Array.Sort(sorted);
            var p95 = sorted[(int)(sorted.Length * 0.95)];

            var timeout = TimeSpan.FromTicks((long)(p95.Ticks * _timeoutMultiplier));
            return Clamp(timeout, MinTimeout, MaxTimeout);
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (observation.Outcome == RequestOutcome.Success)
            {
                _recentSuccesses.Enqueue(observation.Elapsed);
                if (_recentSuccesses.Count > MaxSamples)
                    _recentSuccesses.Dequeue();

                _consecutiveTimeouts = 0;

                if (_recentSuccesses.Count >= 10)
                    _timeoutMultiplier = Math.Max(1.1, _timeoutMultiplier - 0.01);

                return RetryDecision.NoRetry();
            }

            _consecutiveTimeouts++;

            _timeoutMultiplier = Math.Min(2.0, _timeoutMultiplier + 0.1);

            if (observation.Attempt >= 3)
                return RetryDecision.NoRetry();

            var baseDelayMs = 50.0 * Math.Pow(2, observation.Attempt - 1);
            var jitterMs = random.NextDouble() * 0.2 * baseDelayMs;
            var totalDelayMs = baseDelayMs + jitterMs;

            return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(totalDelayMs));
        }

        private static TimeSpan Clamp(TimeSpan value, TimeSpan min, TimeSpan max)
        {
        if (value<min) return min;
        if (value > max) return max;
        return value;
        }
    }
}
