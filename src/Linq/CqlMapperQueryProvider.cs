using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;

namespace FluentCassandra.Linq
{
	public class CqlMapperQueryProvider : IOrderedQueryable, IQueryProvider
	{
		private IDbConnection _conn;
		private string _table;

		public CqlMapperQueryProvider(IDbConnection conn, string table)
		{
			if (conn == null)
				throw new ArgumentNullException("conn");

			_conn = conn;
			_table = table;
		}

		public object Get(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var result = CqlMapperQueryEvaluator.GetEvaluator(expression);
			var obj = CqlMapper.Query(_conn, result.Query, result.Parameters);

			return obj;
		}

		public IEnumerable<TResult> Get<TResult>(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var result = CqlMapperQueryEvaluator.GetEvaluator(expression);
			var obj = CqlMapper.Query<TResult>(_conn, result.Query, result.Parameters);

			return obj;
		}

		public TResult GetFirst<TResult>(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var result = CqlMapperQueryEvaluator.GetEvaluator(expression);
			var obj = CqlMapper.Query<TResult>(_conn, result.Query, result.Parameters).FirstOrDefault();

			return obj;
		}

		public string Table
		{
			get
			{
				return _table;
			}
		}

		public CqlMapperQuery ToQuery()
		{
			var queryable = (IQueryable)this;
			return new CqlMapperQuery(queryable.Expression, this);
		}

		public override string ToString()
		{
			return _table;
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

			if (!typeof(IQueryable<TElement>).IsAssignableFrom(expression.Type))
				throw new ApplicationException("'expression' is not assignable from this type of repository.");

			return new CqlMapperQuery<TElement>(expression, this);
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

			return new CqlMapperQuery(expression, this);
		}

		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		TResult IQueryProvider.Execute<TResult>(Expression expression)
		{
			return GetFirst<TResult>(expression);
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
			return Get(expression);
		}
		#endregion
	}

	public class CqlMapperQueryProvider<T> : CqlMapperQueryProvider, IOrderedQueryable<T>
	{
		public CqlMapperQueryProvider(IDbConnection conn, string table = null)
			: base(conn, table ?? typeof(T).Name) { }

		#region IEnumerable<T> Members

		/// <summary>
		/// Returns an enumerator that iterates through the collection.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
		/// </returns>
		public IEnumerator<T> GetEnumerator()
		{
			var queryable = (IQueryable)this;
			return Get<T>(queryable.Expression).GetEnumerator();
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
			get { return typeof(T); }
		}

		#endregion
	}
}
