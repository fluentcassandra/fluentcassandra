using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Operations;
using FluentCassandra.Linq;

namespace FluentCassandra.Operations
{
	public static class QueryableExtensions
	{
		public static IQueryable<TResult> AsQueryable<TResult>(this QueryableColumnFamilyOperation<TResult> op, BaseCassandraColumnFamily family)
		{
			return new SliceQuery<TResult>(op, family);
		}
	}
}
