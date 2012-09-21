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

		private const int TicksInOneSecond = 10000000;

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
			var elapsedSeconds = _stopwatch.ElapsedTicks / (double)Stopwatch.Frequency;
			
			if (elapsedSeconds > _syncSeconds) {
				Syncronize();
				return _baseTime;
			}

			var elapsedTicks = Convert.ToInt64(elapsedSeconds * TicksInOneSecond);
			return _baseTime.AddTicks(elapsedTicks);
		}
	}
}
