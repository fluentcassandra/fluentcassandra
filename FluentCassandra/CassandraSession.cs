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

		#region Cassandra Descriptions For Server

		public static IEnumerable<CassandraKeyspace> DescribeKeyspacesFor(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				IEnumerable<string> keyspaces = session.GetClient().describe_keyspaces();

				foreach (var keyspace in keyspaces)
					yield return new CassandraKeyspace(keyspace);
			}
		}

		public static string DescribeClusterNameFor(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				string response = session.GetClient().describe_cluster_name();
				return response;
			}
		}

		public static string DescribeVersionFor(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				string response = session.GetClient().describe_version();
				return response;
			}
		}

		public static string DescribePartitionerFor(Server server)
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

			Current = this;
		}

		public CassandraSession(Server server)
			: this()
		{
			_connection = new Connection(server);
		}

		/// <summary>
		/// 
		/// </summary>
		public ConsistencyLevel ReadConsistency { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public ConsistencyLevel WriteConsistency { get; private set; }

		/// <summary>
		/// Gets ConnectionProvider.
		/// </summary>
		public IConnectionProvider ConnectionProvider { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			if (_connection == null || !_connection.IsOpen)
				_connection = ConnectionProvider.Open();

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
			if (!this._disposed && disposing)
			{
				if (this._connection != null)
					this.ConnectionProvider.Close(this._connection);

				if (Current == this)
					Current = null;
			}

			this._disposed = true;
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="Mongo"/> class. 
		/// </summary>
		~CassandraSession()
		{
			this.Dispose(false);
		}

		#endregion
	}
}
