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
		internal CassandraSlicePredicateQuery(QueryableColumnFamilyOperation<TResult, CompareWith> op, BaseCassandraColumnFamily family)
		{
			Operation = op;
			Family = family;
		}

		#region IEnumerable<TResult> Members

		public IEnumerator<TResult> GetEnumerator()
		{
			return (Execute(Expression)).GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region ICassandraQueryable<TResult> Members

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
			get { throw new NotImplementedException(); }
		}

		public ICassandraQueryable<TResult, CompareWith> CreateQuery(Expression expression)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<TResult> Execute(Expression expression)
		{
			throw new NotImplementedException();
		}

		#endregion

		#region IEnumerable<TResult> Members

		IEnumerator<TResult> IEnumerable<TResult>.GetEnumerator()
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}
