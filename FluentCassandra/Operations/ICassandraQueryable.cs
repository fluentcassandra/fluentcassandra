using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public interface ICassandraQueryable<TResult, CompareWith> : IEnumerable<TResult>
		where CompareWith : CassandraType
	{
		ICassandraQueryProvider Provider { get; }
		QueryableColumnFamilyOperation<TResult, CompareWith> Operation { get; }

		Expression Expression { get; }
	}

	public interface ICassandraQueryProvider
	{
		ICassandraQueryable<TResult, CompareWith> CreateQuery<TResult, CompareWith>(QueryableColumnFamilyOperation<TResult, CompareWith> op, Expression expression) where CompareWith : CassandraType;
		IEnumerable<TResult> ExecuteQuery<TResult, CompareWith>(ICassandraQueryable<TResult, CompareWith> query) where CompareWith : CassandraType;
	}
}
