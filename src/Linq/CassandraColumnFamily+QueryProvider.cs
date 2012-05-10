using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FluentCassandra.Types;
using FluentCassandra.Linq;

namespace FluentCassandra
{
	public partial class CassandraColumnFamily : IQueryable, IQueryable<ICqlRow>, IQueryProvider, ICassandraColumnFamilyInfo
	{
		public CqlObjectQueryProvider<T> AsObjectQueryable<T>()
		{
			return new CqlObjectQueryProvider<T>(this);
		}

		public CqlQuery ToQuery()
		{
			var queryable = (IQueryable)this;
			return new CqlQuery(queryable.Expression, this);
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
			return ToQuery().GetEnumerator();
		}

		#endregion

		#region IEnumerable<ICqlRow> Members

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		IEnumerator<ICqlRow> IEnumerable<ICqlRow>.GetEnumerator()
		{
			return ToQuery().GetEnumerator();
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
			get { return typeof(IFluentBaseColumnFamily); }
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

		#region IQueryProvider Members

		/// <summary>
		/// Creates the query.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (typeof(ICqlRow) != typeof(TElement))
				throw new ApplicationException("The resulting column type must be " + typeof(CassandraObject));

			return (IQueryable<TElement>)new CqlQuery(expression, this);
		}

		/// <summary>
		/// Constructs an <see cref="T:System.Linq.IQueryable"/> object that can evaluate the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// An <see cref="T:System.Linq.IQueryable"/> that can evaluate the query represented by the specified expression tree.
		/// </returns>
		IQueryable IQueryProvider.CreateQuery(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			return new CqlQuery(expression, this);
		}

		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		TResult IQueryProvider.Execute<TResult>(Expression expression)
		{
			if (!typeof(TResult).IsAssignableFrom(typeof(ICqlRow)))
				throw new CassandraException("'TElement' must inherit from IFluentBaseColumnFamily");

			if (expression.NodeType == ExpressionType.Call)
				expression = ((MethodCallExpression)expression).Arguments[0];

			var result = new CqlQuery(expression, this);
			return (TResult)Enumerable.FirstOrDefault(result);
		}

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
		object IQueryProvider.Execute(Expression expression)
		{
			return new CqlQuery(expression, this).GetEnumerator();
		}

		#endregion
	}
}