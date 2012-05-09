using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace FluentCassandra.Linq
{
	public class CqlObjectQueryProvider<T> : IQueryable, IQueryable<T>, IQueryProvider
	{
		private readonly CassandraColumnFamily _family;

		public CqlObjectQueryProvider(CassandraColumnFamily family)
		{
			_family = family;
		}

		#region IQueryable Members

		/// <summary>
		/// Gets the type of the element(s) that are returned when the expression tree associated with this instance of <see cref="T:System.Linq.IQueryable"/> is executed.
		/// </summary>
		/// <value></value>
		/// <returns>
		/// A <see cref="T:System.Type"/> that represents the type of the element(s) that are returned when the expression tree associated with this object is executed.
		/// </returns>
		public virtual Type ElementType
		{
			get { return typeof(T); }
		}

		/// <summary>
		/// Gets the expression tree that is associated with the instance of <see cref="T:System.Linq.IQueryable"/>.
		/// </summary>
		/// <value></value>
		/// <returns>
		/// The <see cref="T:System.Linq.Expressions.Expression"/> that is associated with this instance of <see cref="T:System.Linq.IQueryable"/>.
		/// </returns>
		Expression IQueryable.Expression
		{
			get { return Expression.Constant(this); }
		}

		/// <summary>
		/// Gets the query provider that is associated with this data source.
		/// </summary>
		/// <value></value>
		/// <returns>
		/// The <see cref="T:System.Linq.IQueryProvider"/> that is associated with this data source.
		/// </returns>
		IQueryProvider IQueryable.Provider
		{
			get { return this; }
		}


		#endregion

		#region IEnumerable Members

		public System.Collections.IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}

		#endregion

		#region IEnumerable<T> Members

		IEnumerator<T> IEnumerable<T>.GetEnumerator()
		{
			throw new NotImplementedException();
		}

		#endregion

		#region IQueryProvider Members

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
		{
			throw new NotImplementedException();
		}

		public IQueryable CreateQuery(Expression expression)
		{
			throw new NotImplementedException();
		}

		public TResult Execute<TResult>(Expression expression)
		{
			throw new NotImplementedException();
		}

		public object Execute(Expression expression)
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}
