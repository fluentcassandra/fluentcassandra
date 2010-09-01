using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;
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
		/// Verifies that the family passed in is part of this family.
		/// </summary>
		/// <param name="family"></param>
		/// <returns></returns>
		public bool IsPartOfFamily(IFluentBaseColumnFamily family)
		{
			return String.Equals(family.FamilyName, FamilyName);
		}

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(ColumnFamilyOperation<TResult> action)
		{
			return ExecuteOperation<TResult>(action, false);
		}

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <param name="throwOnError"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(ColumnFamilyOperation<TResult> action, bool throwOnError)
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

				if (!success && throwOnError)
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

		ICassandraQueryable<TResult, CompareWith> ICassandraQueryProvider.CreateQuery<TResult, CompareWith>(QueryableColumnFamilyOperation<TResult, CompareWith> op, Expression expression)
		{
			return new CassandraSlicePredicateQuery<TResult, CompareWith>(op, this, expression);
		}

		IEnumerable<TResult> ICassandraQueryProvider.ExecuteQuery<TResult, CompareWith>(ICassandraQueryable<TResult, CompareWith> query)
		{
			query.BuildPredicate();
			return ExecuteOperation(query.Operation, true);
		}

		#endregion
	}
}