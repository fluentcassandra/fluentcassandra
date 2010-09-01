using System;
using System.Collections.Generic;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using System.Collections;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal class CassandraSlicePredicateQuery<TResult, CompareWith> : ICassandraQueryable<TResult, CompareWith>
		where CompareWith : CassandraType
	{
		internal CassandraSlicePredicateQuery(CassandraQuerySetup<TResult, CompareWith> setup, BaseCassandraColumnFamily family, Expression expression)
		{
			Setup = setup;
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

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region ICassandraQueryable<TResult, CompareWith> Members

		public CassandraQuerySetup<TResult, CompareWith> Setup
		{
			get;
			private set;
		}

		public ICassandraQueryProvider Provider
		{
			get { return Family; }
		}

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

		#region ICassandraQueryable Members

		CassandraQuerySetup ICassandraQueryable.Setup
		{
			get { return Setup; }
		}

		#endregion
	}
}
