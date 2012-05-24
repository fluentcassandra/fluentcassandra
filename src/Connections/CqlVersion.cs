using System;
using System.Linq;

namespace FluentCassandra.Connections
{
	public sealed class CqlVersion
	{
		public const string Cql1 = "1.0.0.0";
		public const string Cql2 = "2.0.0.0";
		public const string Cql3 = "3.0.0.0";

		public const string ServerDefault = null;
		public const string Edge = Cql3;
	}
}
