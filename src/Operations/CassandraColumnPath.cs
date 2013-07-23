using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraColumnPath : CassandraColumnParent
	{
		public CassandraObject Column { get; set; }
	}
}
