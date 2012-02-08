using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

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
