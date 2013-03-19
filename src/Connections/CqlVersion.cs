using System;
using System.Linq;

namespace FluentCassandra.Connections
{
	public static class CqlVersion
	{
		public const string Cql = "2.0.0";
		public const string Cql3 = "3.0.0";
		public const string Edge = Cql3;

		[Obsolete("This is no longer supported, please use ConnectionDefault", error: true)]
		public const string ServerDefault = null;
	}
}
