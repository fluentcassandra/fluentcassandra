using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
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

		internal override SlicePredicate CreateSlicePredicate()
		{
			return new SlicePredicate {
				Column_names = Columns.Select(x => (byte[])x).ToList()
			};
		}
	}
}
