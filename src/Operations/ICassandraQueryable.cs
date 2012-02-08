using System;
using System.Collections.Generic;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public interface ICassandraQueryable
	{
		ICassandraQueryProvider Provider { get; }
		CassandraQuerySetup Setup { get; }
		Expression Expression { get; }
	}

	public interface ICassandraQueryable<TResult, CompareWith> : ICassandraQueryable, IEnumerable<TResult>
		where CompareWith : CassandraObject
	{
		new CassandraQuerySetup<TResult, CompareWith> Setup { get; }
	}
}