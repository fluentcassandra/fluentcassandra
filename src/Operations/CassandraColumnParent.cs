using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraColumnParent
	{
		public string ColumnFamily { get; set; }
		public CassandraType SuperColumn { get; set; }
	}
}