using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraSession : IDisposable
	{
		[ThreadStatic]
		private static CassandraSession _current;

		public static CassandraSession Current
		{
			get { return _current; }
			internal set { _current = value; }
		}

		#region Cassandra System For Server

		public static string AddColumnFamily(Server server, CfDef definition)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_add_column_family(definition);
			}
		}

		public static string DropColumnFamily(Server server, string columnFamily)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_drop_column_family(columnFamily);
			}
		}

		public static string RenameColumnFamily(Server server, string oldColumnFamily, string newColumnFamily)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_rename_column_family(oldColumnFamily, newColumnFamily);
			}
		}

		public static string AddKeyspace(Server server, KsDef definition)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_add_keyspace(definition);
			}
		}

		public static string DropKeyspace(Server server, string keyspace)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_drop_keyspace(keyspace);
			}
		}

		public static string RenameKeyspace(Server server, string oldKeyspace, string newKeyspace)
		{
			using (var session = new CassandraSession(server))
			{
				return session.GetClient().system_rename_keyspace(oldKeyspace, newKeyspace);
			}
		}

		#endregion

		#region Cassandra Descriptions For Server

		public static IEnumerable<CassandraKeyspace> DescribeKeyspaces(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				IEnumerable<string> keyspaces = session.GetClient().describe_keyspaces();

				foreach (var keyspace in keyspaces)
					yield return new CassandraKeyspace(keyspace);
			}
		}

		public static string DescribeClusterName(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				string response = session.GetClient().describe_cluster_name();
				return response;
			}
		}

		public static string DescribeVersion(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				string response = session.GetClient().describe_version();
				return response;
			}
		}

		public static string DescribePartitioner(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				string response = session.GetClient().describe_partitioner();
				return response;
			}
		}

		#endregion

		private bool _disposed;
		private IConnection _connection;

		public CassandraSession()
			: this(CassandraContext.CurrentConnectionBuilder) { }

		public CassandraSession(ConnectionBuilder connectionBuilder)
			: this(ConnectionProviderFactory.Get(connectionBuilder), connectionBuilder.ReadConsistency, connectionBuilder.WriteConsistency) { }

		public CassandraSession(ConsistencyLevel read, ConsistencyLevel write)
			: this(ConnectionProviderFactory.Get(CassandraContext.CurrentConnectionBuilder), read, write) { }

		public CassandraSession(IConnectionProvider connectionProvider, ConsistencyLevel read, ConsistencyLevel write)
		{
			if (Current != null)
				throw new CassandraException("Cannot create a new session while there is one already active.");

			ConnectionProvider = connectionProvider;
			ReadConsistency = read;
			WriteConsistency = write;
			Keyspace = new CassandraKeyspace(connectionProvider.Builder.Keyspace);

			Current = this;
		}

		public CassandraSession(Server server)
		{
			_connection = new Connection(server);
		}

		/// <summary>
		/// Gets ConnectionProvider.
		/// </summary>
		public IConnectionProvider ConnectionProvider { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public ConsistencyLevel ReadConsistency { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public ConsistencyLevel WriteConsistency { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public CassandraKeyspace Keyspace { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			if (_connection == null)
			{
				_connection = ConnectionProvider.Open();
				_connection.SetKeyspace(Keyspace.KeyspaceName);
			}

			if (!_connection.IsOpen)
				_connection.Open();

			return _connection.Client;
		}

		#region IDisposable Members

		/// <summary>
		/// 
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}

		/// <summary>
		/// The dispose.
		/// </summary>
		/// <param name="disposing">
		/// The disposing.
		/// </param>
		protected virtual void Dispose(bool disposing)
		{
			if (!_disposed && disposing)
			{
				if (_connection != null)
					ConnectionProvider.Close(_connection);

				if (Current == this)
					Current = null;
			}

			_disposed = true;
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="CassandraSession"/> class. 
		/// </summary>
		~CassandraSession()
		{
			Dispose(false);
		}

		#endregion
	}
}
