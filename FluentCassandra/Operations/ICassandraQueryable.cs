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
		BaseCassandraColumnFamily Family { get; }
		QueryableColumnFamilyOperation<TResult, CompareWith> Operation { get; }

		Expression Expression { get; }
		ICassandraQueryable<TResult, CompareWith> CreateQuery(Expression expression);
		IEnumerable<TResult> Execute(Expression expression);
	}
}
