using System;
using System.Collections.Generic;
using FluentCassandra.Types;
using FluentCassandra.Operations;
using System.Linq.Expressions;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public abstract class BaseCassandraColumnFamily : ICassandraQueryProvider
	{
		private CassandraContext _context;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public BaseCassandraColumnFamily(CassandraContext context, string columnFamily)
		{
			_context = context;
			FamilyName = columnFamily;
			ThrowErrors = context.ThrowErrors;
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
		/// The last error that occured during the execution of an operation.
		/// </summary>
		public CassandraException LastError { get; private set; }

		/// <summary>
		/// Indicates if errors should be thrown when occuring on opperation.
		/// </summary>
		public bool ThrowErrors { get; set; }

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
		/// Removes all the rows from the given column family.
		/// </summary>
		public void RemoveAllRows()
		{
			var op = new Truncate();
			ExecuteOperation(op);
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
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				LastError = null;

				TResult result;
				bool success = action.TryExecute(this, out result);

				if (!success)
					LastError = action.Error;

				if (!success && (throwOnError ?? ThrowErrors))
					throw action.Error;

				return result;
			}
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		#region ICassandraQueryProvider Members

		ICassandraQueryable<TResult, CompareWith> ICassandraQueryProvider.CreateQuery<TResult, CompareWith>(CassandraQuerySetup<TResult, CompareWith> setup, Expression expression) 
		{
			return new CassandraSlicePredicateQuery<TResult, CompareWith>(setup, this, expression);
		}

		IEnumerable<TResult> ICassandraQueryProvider.ExecuteQuery<TResult, CompareWith>(ICassandraQueryable<TResult, CompareWith> query)
		{
			var op = query.BuildQueryableOperation();
			return ExecuteOperation(op, true);
		}

		TResult ICassandraQueryProvider.Execute<TResult>(ICassandraQueryable query, Func<CassandraQuerySetup, CassandraSlicePredicate, ColumnFamilyOperation<TResult>> createOp)
		{
			var op = query.BuildOperation<TResult>(createOp);
			return ExecuteOperation(op, true);
		}

		#endregion
	}
}