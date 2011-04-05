using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Connections;

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

		public static KsDef GetKeyspace(Server server, string keyspace)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(keyspace, server.Host, server.Port)))
				return session.GetClient().describe_keyspace(keyspace);
		}

		public static string AddKeyspace(Server server, KsDef definition)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
				return session.GetClient(setKeyspace: false).system_add_keyspace(definition);
		}

		public static string UpdateKeyspace(Server server, KsDef definition)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
				return session.GetClient(setKeyspace: false).system_update_keyspace(definition);
		}

		public static string DropKeyspace(Server server, string keyspace)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
				return session.GetClient(setKeyspace: false).system_drop_keyspace(keyspace);
		}

		#endregion

		#region Cassandra Descriptions For Server

		public static bool KeyspaceExists(Server server, string keyspaceName)
		{
			return DescribeKeyspaces(server).Any(keyspace => keyspace.KeyspaceName == keyspaceName);
		}

		public static IEnumerable<CassandraKeyspace> DescribeKeyspaces(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				IEnumerable<KsDef> keyspaces = session.GetClient(setKeyspace: false).describe_keyspaces();

				foreach (var keyspace in keyspaces)
					yield return new CassandraKeyspace(keyspace);
			}
		}

		public static string DescribeClusterName(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				string response = session.GetClient(setKeyspace: false).describe_cluster_name();
				return response;
			}
		}

		public static Dictionary<string, List<string>> DescribeSchemaVersions(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				var response = session.GetClient(setKeyspace: false).describe_schema_versions();
				return response;
			}
		}

		public static string DescribeVersion(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				string response = session.GetClient(setKeyspace: false).describe_version();
				return response;
			}
		}

		public static string DescribePartitioner(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				string response = session.GetClient(setKeyspace: false).describe_partitioner();
				return response;
			}
		}

		public static string DescribeSnitch(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(null, server.Host, server.Port)))
			{
				string response = session.GetClient(setKeyspace: false).describe_snitch();
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

			IsAuthenticated = false;
			Current = this;
		}

		public IConnectionProvider ConnectionProvider { get; private set; }

		public ConsistencyLevel ReadConsistency { get; private set; }

		public ConsistencyLevel WriteConsistency { get; private set; }

		public CassandraKeyspace Keyspace { get; private set; }

		public bool IsAuthenticated { get; private set; }

		internal Cassandra.Client GetClient(bool setKeyspace = true)
		{
			if (_connection == null)
				_connection = ConnectionProvider.Open();

			if (!_connection.IsOpen)
				_connection.Open();

			if (setKeyspace)
				_connection.SetKeyspace(Keyspace.KeyspaceName);

			return _connection.Client;
		}

		public void Login(string username, string password)
		{
			var auth = new AuthenticationRequest {
				Credentials = new Dictionary<string, string> { { "username", username }, { "password", password } }
			};

			try
			{
				GetClient().login(auth);
				IsAuthenticated = true;
			}
			catch (Exception exc)
			{
				IsAuthenticated = false;
				throw new CassandraException("Login failed.", exc);
			}
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
