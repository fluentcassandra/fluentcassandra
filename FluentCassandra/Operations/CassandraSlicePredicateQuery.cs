using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using System.Collections;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal class CassandraSlicePredicateQuery<TResult, CompareWith> : ICassandraQueryable<TResult, CompareWith>
		where CompareWith : CassandraType
	{
		internal CassandraSlicePredicateQuery(QueryableColumnFamilyOperation<TResult, CompareWith> op, BaseCassandraColumnFamily family, Expression expression)
		{
			Operation = op;
			Family = family;
			Expression = expression;

			if (Expression == null)
				Expression = Expression.Constant(this);
		}

		#region IEnumerable<TResult> Members

		public IEnumerator<TResult> GetEnumerator()
		{
			return Provider.ExecuteQuery(this).GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region ICassandraQueryable<TResult> Members

		public ICassandraQueryProvider Provider
		{
			get { return Family; }
		}

		public BaseCassandraColumnFamily Family
		{
			get;
			private set;
		}

		public QueryableColumnFamilyOperation<TResult, CompareWith> Operation
		{
			get;
			private set;
		}

		public Expression Expression
		{
			get;
			private set;
		}

		#endregion
	}
}
