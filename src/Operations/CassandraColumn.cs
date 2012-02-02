using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraColumn
	{
		public CassandraType Name { get; set; }
		public CassandraType Value { get; set; }
		public DateTimeOffset Timestamp { get; set; }
		public int? Ttl { get; set; }
	}
}
