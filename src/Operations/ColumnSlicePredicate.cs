using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ColumnSlicePredicate : CassandraSlicePredicate
	{
		public ColumnSlicePredicate(IEnumerable<CassandraType> columns)
		{
			Columns = columns;
		}

		public IEnumerable<CassandraType> Columns { get; private set; }
	}
}
