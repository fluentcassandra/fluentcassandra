using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Operations
{
	public abstract class QueryableColumnFamilyOperation<TResult> : ColumnFamilyOperation<IEnumerable<TResult>>
	{
		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }
	}
}
