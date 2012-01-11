using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraCounterColumn
	{
		public CassandraType Name { get; set; }
		public long Value { get; set; }
	}
}
