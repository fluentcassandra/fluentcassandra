using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;
using FluentCassandra.Types;
using FluentCassandra.Actions;

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
		/// If set contains the super column name that we are querying against.
		/// </summary>
		protected internal CassandraType SuperColumnName { get; protected set; }

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
		public bool IsPartOfFamily(IFluentColumnFamily family)
		{
			return String.Equals(family.FamilyName, FamilyName);
		}

		/// <summary>
		/// Execute the column family action against the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <returns></returns>
		public TResult ExecuteAction<TResult>(ColumnFamilyAction<TResult> action)
		{
			TResult result;
			action.TryExecute(this, out result);
			return result;
		}
	}
}