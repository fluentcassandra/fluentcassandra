using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public abstract class QueryableColumnFamilyOperation<TResult, CompareWith> : ColumnFamilyOperation<IEnumerable<TResult>>
		where CompareWith : CassandraType
	{
		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }
	}
}
