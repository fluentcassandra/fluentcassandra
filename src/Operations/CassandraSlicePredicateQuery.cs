using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentCassandra.Operations
{
	public class CassandraSlicePredicateQuery<TResult> : IEnumerable<TResult>
	{
		internal CassandraSlicePredicateQuery(BaseCassandraColumnFamily family, Expression expression)
		{
			Family = family;
			Expression = expression;

			if (Expression == null)
				Expression = Expression.Constant(this);
		}

		#region IEnumerable<TResult> Members

		public IEnumerator<TResult> GetEnumerator()
		{
			return Family.ExecuteCassandraSlicePredicateQuery(this).GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region CassandraSlicePredicateQuery<TResult> Members

		public BaseCassandraColumnFamily Family
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
