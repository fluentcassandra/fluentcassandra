using System;
using System.Linq;

namespace FluentCassandra
{
	public static class TimestampHelper
	{
		public static readonly DateTimeOffset UnixStart;
		public static readonly long MaxUnixSeconds;
		public static readonly long MaxUnixMilliseconds;
		public static readonly long MaxUnixMicroseconds;

		public const long TicksInOneMicrosecond = 10L;
		public const long TicksInOneMillisecond = 10000L;
		public const long TicksInOneSecond = 10000000L;

		static TimestampHelper()
		{
			UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
			MaxUnixSeconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).TotalSeconds);
			MaxUnixMilliseconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).TotalMilliseconds);
			MaxUnixMicroseconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).Ticks / TicksInOneMicrosecond);
		}

		/// <summary>
		/// Allows for the use of alternative timestamp providers.
		/// </summary>
		public static Func<DateTimeOffset> UtcNow = () => DateTimePrecise.UtcNowOffset;

		public static long ToCassandraTimestamp(this DateTimeOffset dt)
		{
			// we are using the microsecond format from 1/1/1970 00:00:00 UTC same as the Cassandra server
			return (dt - UnixStart).Ticks / TicksInOneMicrosecond;
		}

		public static DateTimeOffset FromCassandraTimestamp(long ts)
		{
			// convert a timestamp in seconds to ticks
			// ** this should never happen, but it is in here for good measure **
			if (ts <= MaxUnixSeconds)
				ts *= TicksInOneSecond;

			// convert a timestamp in milliseconds to ticks
			if (ts <= MaxUnixMilliseconds)
				ts *= TicksInOneMillisecond;

			// convert a timestamp in microseconds to ticks
			if (ts <= MaxUnixMicroseconds)
				ts *= TicksInOneMicrosecond;

			return UnixStart.AddTicks(ts);
		}
	}
}
