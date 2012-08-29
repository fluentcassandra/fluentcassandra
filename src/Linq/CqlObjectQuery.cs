using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FluentCassandra.ObjectSerializer;

namespace FluentCassandra.Linq
{
	public class CqlObjectQuery<T> : IQueryable, IQueryable<T>, IOrderedQueryable, IOrderedQueryable<T>
	{
		private readonly Expression _expression;
		private readonly IQueryProvider _provider;
		private readonly CassandraColumnFamily _family;

		/// <summary>
		/// Initializes a new instance of the <see cref="CqlQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlObjectQuery(Expression expression, IQueryProvider provider, CassandraColumnFamily family)
		{
			_expression = expression;
			_provider = provider;
			_family = family;
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraColumnFamily Family
		{
			get { return _family; }
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
		public IEnumerator<T> GetEnumerator()
		{
			var result = CqlQueryEvaluator.GetCql(Expression);
			var fluentObjects = _family.Context.ExecuteQuery(result);

			var serializer = ObjectSerializerFactory.Get(typeof(T));
			return serializer
				.Deserialize(fluentObjects)
				.OfType<T>()
				.GetEnumerator();
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
			get { return typeof(T); }
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
		public IQueryProvider Provider
		{
			get { return _provider; }
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