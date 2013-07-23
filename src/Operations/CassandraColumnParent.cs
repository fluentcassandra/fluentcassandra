using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraColumnParent
	{
		public string ColumnFamily { get; set; }
		public CassandraObject SuperColumn { get; set; }
	}
}