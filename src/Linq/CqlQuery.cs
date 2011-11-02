using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FluentCassandra.Types;
using FluentCassandra.Operations;

namespace FluentCassandra.Linq
{
	public class CqlQuery<CompareWith> : IQueryable, IQueryable<ICqlRow<CompareWith>>
		where CompareWith : CassandraType
	{
		private readonly Expression _expression;
		private readonly CassandraColumnFamily<CompareWith> _provider;

		/// <summary>
		/// Initializes a new instance of the <see cref="CqlQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <param name="provider">The provider.</param>
		public CqlQuery(Expression expression, CassandraColumnFamily<CompareWith> provider)
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

		#region IEnumerable<ICqlRow<CompareWith>> Members

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IEnumerator<ICqlRow<CompareWith>> GetEnumerator()
		{
			var result = CqlQueryEvaluator<CompareWith>.GetEvaluator<CompareWith>(Expression);
			var op = new ExecuteCqlQuery<CompareWith>(result.Query);
			return Provider.ExecuteOperation(op).GetEnumerator();
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
		public CassandraColumnFamily<CompareWith> Provider
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
			return CqlQueryEvaluator<CompareWith>.GetCql<CompareWith>(Expression);
		}
	}
}