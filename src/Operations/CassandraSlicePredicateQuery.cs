using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;

namespace FluentCassandra.Operations
{
	public class CassandraSlicePredicateQuery<TResult> : IEnumerable<TResult>
	{
        private IEnumerator<TResult> _internalList;

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
             if (_internalList == null)
                _internalList = Family.ExecuteCassandraSlicePredicateQuery(this).GetEnumerator();

             return _internalList;
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
