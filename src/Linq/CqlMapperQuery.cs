using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace FluentCassandra.Linq
{
	/// <see href="https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile"/>
	public class CqlMapperQuery : IOrderedQueryable
	{
		private readonly Expression _expression;
		private readonly CqlMapperQueryProvider _provider;

		/// <summary>
		/// Initializes a new instance of the <see cref="CqlMapperQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlMapperQuery(Expression expression, CqlMapperQueryProvider provider)
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
			var queryable = (IQueryable)this;
			return ((IEnumerable)queryable.Provider.Execute(queryable.Expression)).GetEnumerator();
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
			get { return typeof(object); }
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
		public CqlMapperQueryProvider Provider
		{
			get { return _provider; }
		}

		IQueryProvider IQueryable.Provider
		{
			get { return Provider; }
		}

		#endregion

		public override string ToString()
		{
			return CqlMapperQueryEvaluator.GetSql(Expression);
		}
	}

	public class CqlMapperQuery<TQuery> : CqlMapperQuery, IOrderedQueryable<TQuery>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="CqlMapperQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlMapperQuery(Expression expression, CqlMapperQueryProvider provider)
			: base(expression, provider) { }

		#region IEnumerable<TQuery> Members

		/// <summary>
		/// Returns an enumerator that iterates through the collection.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
		/// </returns>
		public IEnumerator<TQuery> GetEnumerator()
		{
			if (ElementType.IsAnonymousType())
				throw new NotSupportedException("Please call the AsTypelessQuery() on this query, because anonymous types are not supported.");

			return Provider.Get<TQuery>(Expression).GetEnumerator();
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
		public override Type ElementType
		{
			get
			{
				return typeof(TQuery);
			}
		}
		#endregion
	}
}