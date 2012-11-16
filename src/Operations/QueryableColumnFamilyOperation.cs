using System;
using System.Collections.Generic;

namespace FluentCassandra.Operations
{
	public abstract class QueryableColumnFamilyOperation<TResult> : ColumnFamilyOperation<IList<TResult>>
	{
		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }
	}
}
