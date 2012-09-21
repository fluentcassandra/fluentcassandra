using System;
using System.Diagnostics;
using System.Linq;

namespace System
{
	public class DateTimePrecise
	{
		private static readonly DateTimePrecise Instance = new DateTimePrecise(10);

		public static DateTime Now
		{
			get { return Instance.GetUtcNow().LocalDateTime; }
		}

		public static DateTime UtcNow
		{
			get { return Instance.GetUtcNow().UtcDateTime; }
		}

		public static DateTimeOffset NowOffset
		{
			get { return Instance.GetUtcNow().ToLocalTime(); }
		}

		public static DateTimeOffset UtcNowOffset
		{
			get { return Instance.GetUtcNow(); }
		}

		private const long TicksInOneSecond = 10000000L;

		private readonly double _syncSeconds;
		private readonly Stopwatch _stopwatch;
		private DateTimeOffset _baseTime;

		public DateTimePrecise(int syncSeconds)
		{
			_syncSeconds = syncSeconds;
			_stopwatch = new Stopwatch();

			Syncronize();
		}

		private void Syncronize()
		{
			lock (_stopwatch) {
				_baseTime = DateTimeOffset.UtcNow;
				_stopwatch.Restart();
			}
		}

		public DateTimeOffset GetUtcNow()
		{
			var elapsed = _stopwatch.Elapsed;

			if (elapsed.TotalSeconds > _syncSeconds)
			{
				Syncronize();

				// account for any time that has passed since the stopwatch was syncronized
				elapsed = _stopwatch.Elapsed;
			}

			if (elapsed < TimeSpan.Zero)
				elapsed = TimeSpan.Zero;

			return _baseTime + elapsed;
		}
	}
}
