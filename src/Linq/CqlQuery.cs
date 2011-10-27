using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace FluentCassandra.Linq
{
	/// <see href="https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile"/>
	public class CqlQuery : IQueryable
	{
		private readonly Expression _expression;
		private readonly CqlQueryProvider _provider;

		/// <summary>
		/// Initializes a new instance of the <see cref="CqlQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlQuery(Expression expression, CqlQueryProvider provider)
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
		public CqlQueryProvider Provider
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
			return CqlQueryEvaluator.GetSql(Expression);
		}
	}
}