using System;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public abstract class CassandraSlicePredicate
	{
		internal abstract SlicePredicate CreateSlicePredicate();
	}
}