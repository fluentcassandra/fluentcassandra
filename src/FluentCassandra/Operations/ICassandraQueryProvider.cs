using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public interface ICassandraQueryProvider
	{
		ICassandraQueryable<TResult, CompareWith> CreateQuery<TResult, CompareWith>(CassandraQuerySetup<TResult, CompareWith> setup, Expression expression) where CompareWith : CassandraType;
		IEnumerable<TResult> ExecuteQuery<TResult, CompareWith>(ICassandraQueryable<TResult, CompareWith> query) where CompareWith : CassandraType;
		TResult Execute<TResult>(ICassandraQueryable query,  Func<CassandraQuerySetup, CassandraSlicePredicate, ColumnFamilyOperation<TResult>> createOp);
	}
}
