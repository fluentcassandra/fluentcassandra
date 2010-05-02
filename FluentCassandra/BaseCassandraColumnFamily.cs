using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;
using FluentCassandra.Types;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public abstract class BaseCassandraColumnFamily
	{
		private CassandraContext _context;
		private CassandraKeyspace _keyspace;
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public BaseCassandraColumnFamily(CassandraContext context, CassandraKeyspace keyspace, IConnection connection, string columnFamily)
		{
			_context = context;
			_keyspace = keyspace;
			_connection = connection;
			FamilyName = columnFamily;
		}

		/// <summary>
		/// The context the column family currently belongs to.
		/// </summary>
		public CassandraContext Context { get { return _context; } }

		/// <summary>
		/// The keyspace the column family belongs to.
		/// </summary>
		public CassandraKeyspace Keyspace { get { return _keyspace; } }

		/// <summary>
		/// The family name for this column family.
		/// </summary>
		public string FamilyName { get; private set; }

		/// <summary>
		/// The last error that occured during the execution of an operation.
		/// </summary>
		public CassandraException LastError { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

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
			LastError = null;

			TResult result;
			bool success = action.TryExecute(this, out result);

			if (!success)
				LastError = action.Error;

			if (!success && throwOnError)
				throw action.Error;

			return result;
		}
	}
}