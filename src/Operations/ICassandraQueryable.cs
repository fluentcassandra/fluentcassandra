using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentCassandra.Operations
{
	public interface CassandraSlicePredicateQuery<TResult> : IEnumerable<TResult>
	{
		ICassandraQueryProvider Provider { get; }
		Expression Expression { get; }
	}
}