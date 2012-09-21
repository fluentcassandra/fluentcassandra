using System;
using System.Diagnostics;
using System.Linq;

namespace System
{
	public class DateTimePrecise
	{
		private static readonly DateTimePrecise Instance = new DateTimePrecise();

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

		private readonly double _divergentSeconds;
		private readonly double _syncSeconds;
		private readonly Stopwatch _stopwatch;
		private DateTimeOffset _baseTime;

		public DateTimePrecise(int syncSeconds = 1, int divergentSeconds = 1)
		{
			_syncSeconds = syncSeconds;
			_divergentSeconds = divergentSeconds;

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
			var now = DateTimeOffset.UtcNow;
			var elapsed = _stopwatch.Elapsed;

			if (elapsed.TotalSeconds > _syncSeconds)
			{
				Syncronize();

				// account for any time that has passed since the stopwatch was syncronized
				elapsed = _stopwatch.Elapsed;
			}

			/**
			 * The Stopwatch has many bugs associated with it, so when we are in doubt of the results
			 * we are going to default to DateTimeOffset.UtcNow
			 * http://stackoverflow.com/questions/1008345
			 **/

			// check for elapsed being less than zero
			if (elapsed < TimeSpan.Zero)
				return now;

			var preciseNow = _baseTime + elapsed;

			// make sure the two clocks don't diverge by more than defined seconds
			if (Math.Abs((preciseNow - now).TotalSeconds) > _divergentSeconds)
				return now;

			return _baseTime + elapsed;
		}
	}
}
