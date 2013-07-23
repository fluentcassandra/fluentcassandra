using FluentCassandra.Operations;
using FluentCassandra.Types;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public abstract class BaseCassandraColumnFamily
	{
		private readonly CassandraContext _context;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public BaseCassandraColumnFamily(CassandraContext context, string columnFamily)
		{
			_context = context;
			FamilyName = columnFamily;
		}

		/// <summary>
		/// The context the column family currently belongs to.
		/// </summary>
		public CassandraContext Context { get { return _context; } }

		/// <summary>
		/// The family name for this column family.
		/// </summary>
		public string FamilyName { get; private set; }

		/// <summary>
		/// Verifies that the family passed in is part of this family.
		/// </summary>
		/// <param name="family"></param>
		/// <returns></returns>
		public bool IsPartOfFamily(IFluentBaseColumnFamily family)
		{
			return String.Equals(family.FamilyName, FamilyName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public abstract CassandraColumnFamilySchema GetSchema();

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public abstract void SetSchema(CassandraColumnFamilySchema schema);

		/// <summary>
		/// 
		/// </summary>
		public abstract void ClearCachedColumnFamilySchema();

		/// <summary>
		/// 
		/// </summary>
		public void TryCreateSelf()
		{
			Context.Keyspace.TryCreateColumnFamily(GetSchema());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		public void RemoveKey(CassandraObject key)
		{
			var op = new Remove(key);
			ExecuteOperation(op);
		}

		/// <summary>
		/// Removes all the rows from the given column family.
		/// </summary>
		public void RemoveAllRows()
		{
			_context.ExecuteOperation(new SimpleOperation<int>(ctx => {
				ctx.Session.GetClient().truncate(FamilyName);
				return 0;
			}));
		}

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <param name="throwOnError"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(ColumnFamilyOperation<TResult> action, bool? throwOnError = null)
		{
			action.ColumnFamily = this;

			return _context.ExecuteOperation(action, throwOnError);
		}

		public CassandraSlicePredicateQuery<TResult> CreateCassandraSlicePredicateQuery<TResult>(Expression expression) 
		{
			return new CassandraSlicePredicateQuery<TResult>(this, expression);
		}

		public IEnumerable<TResult> ExecuteCassandraSlicePredicateQuery<TResult>(CassandraSlicePredicateQuery<TResult> query)
		{
			var op = query.BuildQueryableOperation();
			return ExecuteOperation((QueryableColumnFamilyOperation<TResult>)op);
		}

		public override string ToString()
		{
			return FamilyName;
		}
	}
}