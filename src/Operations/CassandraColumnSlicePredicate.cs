using FluentCassandra.Types;
using System.Collections.Generic;

namespace FluentCassandra.Operations
{
	public class CassandraColumnSlicePredicate : CassandraSlicePredicate
	{
		public CassandraColumnSlicePredicate(IEnumerable<CassandraObject> columns)
		{
			Columns = columns;
		}

		public IEnumerable<CassandraObject> Columns { get; private set; }
	}
}
