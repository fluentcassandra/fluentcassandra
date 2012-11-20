using System;
using System.Collections.Generic;
using System.Text;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Connections
{
	public class ConnectionBuilder : FluentCassandra.Connections.IConnectionBuilder
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="timeout"></param>
		public ConnectionBuilder(string keyspace, string host, int port = Server.DefaultPort, int connectionTimeout = Server.DefaultTimeout, bool pooling = false, int minPoolSize = 0, int maxPoolSize = 100, int connectionLifetime = 0, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.ServerDefault, bool compressCqlQueries = true, string username = null, string password = null, int maxRetries = 0)
		{
			Keyspace = keyspace;
			Servers = new List<Server>() { new Server(host, port) };
			ConnectionTimeout = TimeSpan.FromSeconds(connectionTimeout);
			Pooling = pooling;
			MinPoolSize = minPoolSize;
			MaxPoolSize = maxPoolSize;
			MaxRetries = maxRetries;
			ConnectionLifetime = TimeSpan.FromSeconds(connectionLifetime);
			ConnectionType = connectionType;
			BufferSize = bufferSize;
			ReadConsistency = read;
			WriteConsistency = write;
			CqlVersion = cqlVersion;
			CompressCqlQueries = compressCqlQueries;
			Username = username;
			Password = password;

			ConnectionString = GetConnectionString();
		}

		public ConnectionBuilder(string keyspace, Server server, bool pooling = false, int minPoolSize = 0, int maxPoolSize = 100, int connectionLifetime = 0, ConnectionType connectionType = ConnectionType.Framed, int bufferSize = 1024, ConsistencyLevel read = ConsistencyLevel.QUORUM, ConsistencyLevel write = ConsistencyLevel.QUORUM, string cqlVersion = FluentCassandra.Connections.CqlVersion.ServerDefault, bool compressCqlQueries = true, string username = null, string password = null, int maxRetries = 0)
		{
			Keyspace = keyspace;
			Servers = new List<Server>() { server };
			ConnectionTimeout = TimeSpan.FromSeconds(server.Timeout);
			Pooling = pooling;
			MinPoolSize = minPoolSize;
			MaxPoolSize = maxPoolSize;
			MaxRetries = maxRetries;
			ConnectionLifetime = TimeSpan.FromSeconds(connectionLifetime);
			ConnectionType = connectionType;
			BufferSize = bufferSize;
			ReadConsistency = read;
			WriteConsistency = write;
			CqlVersion = cqlVersion;
			CompressCqlQueries = compressCqlQueries;
			Username = username;
			Password = password;

			ConnectionString = GetConnectionString();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionString"></param>
		public ConnectionBuilder(string connectionString)
		{
			InitializeConnectionString(connectionString);
			ConnectionString = GetConnectionString();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionString"></param>
		private void InitializeConnectionString(string connectionString)
		{
			string[] connParts = connectionString.Split(';');
			IDictionary<string, string> pairs = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

			foreach (string part in connParts)
			{
				string[] nameValue = part.Split(new[] { '=' }, 2);

				if (nameValue.Length != 2)
					continue;

				pairs.Add(nameValue[0].Trim(), nameValue[1].Trim());
			}

			#region Keyspace

			if (pairs.ContainsKey("Keyspace"))
			{
				Keyspace = pairs["Keyspace"];
			}

			#endregion

			#region Pooling

			if (!pairs.ContainsKey("Pooling"))
			{
				Pooling = false;
			}
			else
			{
				bool pooling;

				if (!Boolean.TryParse(pairs["Pooling"], out pooling))
					pooling = false;

				Pooling = pooling;
			}

			#endregion

			#region MinPoolSize

			if (!pairs.ContainsKey("Min Pool Size"))
			{
				MinPoolSize = 0;
			}
			else
			{
				int minPoolSize;

				if (!Int32.TryParse(pairs["Min Pool Size"], out minPoolSize))
					minPoolSize = 0;

				if (minPoolSize < 0)
					minPoolSize = 0;

				MinPoolSize = minPoolSize;
			}

			#endregion

			#region MaxPoolSize

			if (!pairs.ContainsKey("Max Pool Size"))
			{
				MaxPoolSize = 100;
			}
			else
			{
				int maxPoolSize;

				if (!Int32.TryParse(pairs["Max Pool Size"], out maxPoolSize))
					maxPoolSize = 100;

				if (maxPoolSize < 0)
					maxPoolSize = 100;

				MaxPoolSize = maxPoolSize;
			}

			#endregion

			#region MaxRetries

			if (pairs.ContainsKey("Max Retries"))
			{
				int maxRetries;

				if (!Int32.TryParse(pairs["Max Retries"], out maxRetries))
					maxRetries = 0;

				if (maxRetries < 0)
					maxRetries = 0;

				MaxRetries = maxRetries;
			}
		   
			#endregion

			#region ConnectionTimeout

			if (!pairs.ContainsKey("Connection Timeout"))
			{
				ConnectionTimeout = TimeSpan.Zero;
			}
			else
			{
				int connectionTimeout;

				if (!Int32.TryParse(pairs["Connection Timeout"], out connectionTimeout))
					throw new CassandraException("Connection Timeout is not valid.");

				if (connectionTimeout < 0)
					connectionTimeout = 0;

				ConnectionTimeout = TimeSpan.FromSeconds(connectionTimeout);
			}

			#endregion

			#region ConnectionLifetime

			if (!pairs.ContainsKey("Connection Lifetime"))
			{
				ConnectionLifetime = TimeSpan.Zero;
			}
			else
			{
				int lifetime;

				if (!Int32.TryParse(pairs["Connection Lifetime"], out lifetime))
					lifetime = 0;

				ConnectionLifetime = TimeSpan.FromSeconds(lifetime);
			}

			#endregion

			#region ConnectionType

			if (!pairs.ContainsKey("Connection Type"))
			{
				ConnectionType = ConnectionType.Framed;
			}
			else
			{
				ConnectionType type;

				if (!Enum.TryParse(pairs["Connection Type"], out type))
					ConnectionType = ConnectionType.Framed;

				ConnectionType = type;
			}

			#endregion

			#region BufferSize

			if (!pairs.ContainsKey("Buffer Size"))
			{
				BufferSize = 1024;
			}
			else
			{
				int bufferSize;

				if (!Int32.TryParse(pairs["Buffer Size"], out bufferSize))
					bufferSize = 1024;

				if (bufferSize < 0)
					bufferSize = 1024;

				BufferSize = bufferSize;
			}

			#endregion

			#region Read

			if (!pairs.ContainsKey("Read"))
			{
				ReadConsistency = ConsistencyLevel.QUORUM;
			}
			else
			{
				ConsistencyLevel read;

				if (!Enum.TryParse(pairs["Read"], out read))
					ReadConsistency = ConsistencyLevel.QUORUM;

				ReadConsistency = read;
			}

			#endregion

			#region Write

			if (!pairs.ContainsKey("Write"))
			{
				WriteConsistency = ConsistencyLevel.QUORUM;
			}
			else
			{
				ConsistencyLevel write;

				if (!Enum.TryParse(pairs["Write"], out write))
					WriteConsistency = ConsistencyLevel.QUORUM;

				WriteConsistency = write;
			}

			#endregion

			#region CqlVersion

			if (!pairs.ContainsKey("CQL Version"))
			{
				CqlVersion = FluentCassandra.Connections.CqlVersion.ServerDefault;
			}
			else
			{
				CqlVersion = pairs["CQL Version"];
			}

			#endregion

			#region CompressCqlQueries

			if (!pairs.ContainsKey("Compress CQL Queries"))
			{
				CompressCqlQueries = true;
			}
			else
			{
				string compressCqlQueriesValue = pairs["Compress CQL Queries"];

				// YES or TRUE is a positive response everything else is a negative response
				CompressCqlQueries = String.Equals("yes", compressCqlQueriesValue, StringComparison.OrdinalIgnoreCase) || String.Equals("true", compressCqlQueriesValue, StringComparison.OrdinalIgnoreCase);
			}

			#endregion

			#region Username

			if (pairs.ContainsKey("Username"))
			{
				Username = pairs["Username"];
			}

			#endregion

			#region Password

			if (pairs.ContainsKey("Password"))
			{
				Password = pairs["Password"];
			}

			#endregion

			// This must be last because it uses fields from above
			#region Server

			Servers = new List<Server>();

			if (!pairs.ContainsKey("Server"))
			{
				Servers.Add(new Server());
			}
			else
			{
				string[] servers = pairs["Server"].Split(',');
				foreach (var server in servers)
				{
					string[] serverParts = server.Split(':');
					string host = serverParts[0].Trim();

					if (serverParts.Length == 2)
					{
						int port;
						if (Int32.TryParse(serverParts[1].Trim(), out port))
							Servers.Add(new Server(host: host, port: port, timeout: ConnectionTimeout.Seconds));
						else
							Servers.Add(new Server(host: host, timeout: ConnectionTimeout.Seconds));
					}
					else
						Servers.Add(new Server(host: host, timeout: ConnectionTimeout.Seconds));
				}
			}

			#endregion
		}

		private string GetConnectionString()
		{
			StringBuilder b = new StringBuilder();
			string format = "{0}={1};";

			b.AppendFormat(format, "Keyspace", Keyspace);
			b.AppendFormat(format, "Server", String.Join(",", Servers));

			b.AppendFormat(format, "Pooling", Pooling);
			b.AppendFormat(format, "Min Pool Size", MinPoolSize);
			b.AppendFormat(format, "Max Pool Size", MaxPoolSize);
			b.AppendFormat(format, "Connection Timeout", Convert.ToInt32(ConnectionTimeout.TotalSeconds));
			b.AppendFormat(format, "Connection Lifetime", Convert.ToInt32(ConnectionLifetime.TotalSeconds));
			b.AppendFormat(format, "Connection Type", ConnectionType);

			b.AppendFormat(format, "Buffer Size", BufferSize);
			b.AppendFormat(format, "Read", ReadConsistency);
			b.AppendFormat(format, "Write", WriteConsistency);

			b.AppendFormat(format, "CQL Version", CqlVersion);
			b.AppendFormat(format, "Compress CQL Queries", CompressCqlQueries);

			b.AppendFormat(format, "Username", Username);
			b.AppendFormat(format, "Password", Password);

			return b.ToString();
		}

		/// <summary>
		/// 
		/// </summary>
		public string Keyspace { get; private set; }

		/// <summary>
		/// The length of time (in seconds) to wait for a connection to the server before terminating the attempt and generating an error.
		/// </summary>
		public TimeSpan ConnectionTimeout { get; private set; }

		/// <summary>
		/// When true, the Connection object is drawn from the appropriate pool, or if necessary, is created and added to the appropriate pool. Recognized values are true, false, yes, and no.
		/// </summary>
		public bool Pooling { get; private set; }

		/// <summary>
		/// (Not Currently Implimented) The minimum number of connections allowed in the pool.
		/// </summary>
		public int MinPoolSize { get; private set; }

		/// <summary>
		/// The maximum number of connections allowed in the pool.
		/// </summary>
		public int MaxPoolSize { get; private set; }

		/// <summary>
		/// The maximum number of execution retry attempts if there is an error during the execution of an operation and the exception is a type that can be retried.
		/// </summary>
		public int MaxRetries { get; private set; }

		/// <summary>
		/// When a connection is returned to the pool, its creation time is compared with the current time, and the connection is destroyed if that time span (in seconds) exceeds the value specified by Connection Lifetime. This is useful in clustered configurations to force load balancing between a running server and a server just brought online. A value of zero (0) causes pooled connections to have the maximum connection timeout.
		/// </summary>
		public TimeSpan ConnectionLifetime { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public ConnectionType ConnectionType { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public int BufferSize { get; private set; }

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
		public IList<Server> Servers { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public string CqlVersion { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public bool CompressCqlQueries { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public string Username { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public string Password { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public string ConnectionString { get; private set; }

		/// <summary>
		/// A unique identifier for the connection builder.
		/// </summary>
		public string Uuid { get { return ConnectionString; } }
	}
}