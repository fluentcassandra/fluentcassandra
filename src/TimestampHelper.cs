using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public static class TimestampHelper
	{
		public static readonly DateTimeOffset UnixStart;
		public static readonly long MaxUnixSeconds;
		public static readonly long MaxUnixMilliseconds;
		public static readonly long MaxUnixMicroseconds;

		static TimestampHelper()
		{
			UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
			MaxUnixSeconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).TotalSeconds);
			MaxUnixMilliseconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).TotalMilliseconds);
			MaxUnixMicroseconds = Convert.ToInt64((DateTimeOffset.MaxValue - UnixStart).Ticks / 10L);
		}

		public static long ToCassandraTimestamp(this DateTimeOffset dt)
		{
			// we are using the microsecond format from 1/1/1970 00:00:00 UTC same as the Cassandra server
			return (dt - UnixStart).Ticks / 10L;
		}

		public static DateTimeOffset FromCassandraTimestamp(long ts)
		{
			if (ts <= MaxUnixSeconds)
				ts *= 1000L;

			if (ts <= MaxUnixMilliseconds)
				ts *= 1000L;

			if (ts <= MaxUnixMicroseconds)
				ts *= 10L;

			return UnixStart.AddTicks(ts);
		}
	}
}
