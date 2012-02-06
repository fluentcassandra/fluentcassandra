using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public static class Helper
	{
		public static DateTime MillisecondResolution(this DateTime dt)
		{
			var t = dt - DateTime.MinValue;
			return DateTime.MinValue.AddMilliseconds(t.TotalMilliseconds);
		}

		public static DateTimeOffset MillisecondResolution(this DateTimeOffset dt)
		{
			var t = dt - DateTimeOffset.MinValue;
			return DateTimeOffset.MinValue.AddMilliseconds(t.TotalMilliseconds);
		}
	}
}
