using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace FluentCassandra.Linq
{
	public class CqlQuery : IQueryable, IQueryable<ICqlRow>
	{
		private readonly Expression _expression;
		private readonly CassandraColumnFamily _provider;

		/// <summary>
		/// Initializes a new instance of the <see cref="CqlQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlQuery(Expression expression, CassandraColumnFamily provider)
		{
			_expression = expression;
			_provider = provider;
		}

		#region IEnumerable Members

		/// <summary>
		/// Returns an enumerator that iterates through a collection.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
		/// </returns>
		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region IEnumerable<ICqlRow> Members

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IEnumerator<ICqlRow> GetEnumerator()
		{
			var result = CqlQueryEvaluator.GetCql(Expression);
			return Provider.Cql(result).GetEnumerator();
		}

		#endregion

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
			get { return typeof(ICqlRow); }
		}

		/// <summary>
		/// Gets the expression tree that is associated with the instance of <see cref="T:System.Linq.IQueryable"/>.
		/// </summary>
		/// <value></value>
		/// <returns>
		/// The <see cref="T:System.Linq.Expressions.Expression"/> that is associated with this instance of <see cref="T:System.Linq.IQueryable"/>.
		/// </returns>
		public Expression Expression
		{
			get { return _expression; }
		}

		/// <summary>
		/// Gets the query provider that is associated with this data source.
		/// </summary>
		/// <value></value>
		/// <returns>
		/// The <see cref="T:System.Linq.IQueryProvider"/> that is associated with this data source.
		/// </returns>
		public CassandraColumnFamily Provider
		{
			get { return _provider; }
		}

		/// <summary>
		/// 
		/// </summary>
		IQueryProvider IQueryable.Provider
		{
			get { return Provider; }
		}

		#endregion

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override string ToString()
		{
			return CqlQueryEvaluator.GetCql(Expression);
		}
	}
}