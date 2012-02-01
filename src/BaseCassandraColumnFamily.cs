using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Apache.Cassandra;
using FluentCassandra.Operations;
using FluentCassandra.Types;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public abstract class BaseCassandraColumnFamily : ICassandraQueryProvider
	{
		private CassandraContext _context;
		private CassandraColumnFamilySchema _cachedColumnFamilyFluentFriendlySchema;

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
		/// 
		/// </summary>
		/// <returns></returns>
		public CassandraColumnFamilySchema Schema()
		{
			var def = _context.Keyspace.GetColumnFamilyDescription(FamilyName);

			if (_cachedColumnFamilyFluentFriendlySchema == null)
			{
				var keyType = CassandraType.GetCassandraType(def.Key_validation_class);
				var colNameType = CassandraType.GetCassandraType(def.Default_validation_class);

				_cachedColumnFamilyFluentFriendlySchema = new CassandraColumnFamilySchema {
					FamilyName = FamilyName,
					Key = keyType,
					Columns = def.Column_metadata.ToDictionary(
						col => CassandraType.GetTypeFromDatabaseValue(col.Name, colNameType),
						col => CassandraType.GetCassandraType(col.Validation_class))
				};
			}

			return _cachedColumnFamilyFluentFriendlySchema;
		}

		/// <summary>
		/// 
		/// </summary>
		public void ClearCachedKeyspaceSchema()
		{
			_cachedColumnFamilyFluentFriendlySchema = null;
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
			if (!throwOnError.HasValue)
				throwOnError = ThrowErrors;

			var localSession = CassandraSession.Current;
			if (localSession == null)
				localSession = _context.OpenSession();

			action.Context = _context;
			action.ColumnFamily = this;

			try
			{
				var result = localSession.ExecuteOperation(action, throwOnError);
				LastError = localSession.LastError;

				return result;
			}
			finally
			{
				if (localSession != null)
					localSession.Dispose();
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

		public override string ToString()
		{
			return FamilyName;
		}
	}
}