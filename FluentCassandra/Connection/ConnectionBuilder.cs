using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class ConnectionBuilder
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="timeout"></param>
		/// <param name="provider"></param>
		public ConnectionBuilder(string keyspace, string host, int port = 9160, int timeout = 0, bool pooled = false, int poolSize = 25, int lifetime = 0)
		{
			Keyspace = keyspace;
			Servers = new List<Server>() { new Server(host, port) };
			Timeout = timeout;
			Pooled = pooled;
			PoolSize = poolSize;
			Lifetime = lifetime;
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

				pairs.Add(nameValue[0], nameValue[1]);
			}

			#region Keyspace

			if (pairs.ContainsKey("Keyspace"))
				Keyspace = pairs["Keyspace"];

			#endregion

			#region Timeout

			if (!pairs.ContainsKey("Timeout"))
			{
				Timeout = 0;
			}
			else
			{
				int timeout;

				if (!Int32.TryParse(pairs["Timeout"], out timeout))
					throw new CassandraException("Timeout is not valid.");

				if (timeout < 0)
					timeout = 0;
				
				Timeout = timeout;
			}

			#endregion

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
					string host = serverParts[0];

					if (serverParts.Length == 2)
					{
						int port;
						if (Int32.TryParse(serverParts[1], out port))
							Servers.Add(new Server(host, port));
						else
							Servers.Add(new Server(host));
					}
					else
						Servers.Add(new Server(host));
				}
			}

			#endregion

			#region Pooled

			if (!pairs.ContainsKey("Pooled"))
			{
				Pooled = false;
			}
			else
			{
				bool pooled;

				if (!Boolean.TryParse(pairs["Pooled"], out pooled))
					pooled = false;

				Pooled = pooled;
			}

			#endregion

			#region PoolSize

			if (!pairs.ContainsKey("PoolSize"))
			{
				PoolSize = 25;
			}
			else
			{
				int poolSize;

				if (!Int32.TryParse(pairs["PoolSize"], out poolSize))
					poolSize = 25;

				if (poolSize < 0)
					poolSize = 25;

				PoolSize = poolSize;
			}

			#endregion

			#region Lifetime

			if (!pairs.ContainsKey("Lifetime"))
			{
				Lifetime = 0;
			}
			else
			{
				int lifetime;

				if (!Int32.TryParse(pairs["Lifetime"], out lifetime))
					lifetime = 0;

				Lifetime = lifetime;
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
		}

		private string GetConnectionString()
		{
			StringBuilder b = new StringBuilder();
			string format = "{0}={1};";

			b.AppendFormat(format, "Keyspace", Keyspace);
			b.AppendFormat(format, "Server", String.Join(",", Servers));
			b.AppendFormat(format, "Timeout", Timeout);
			b.AppendFormat(format, "Pooled", Pooled);
			b.AppendFormat(format, "PoolSize", PoolSize);
			b.AppendFormat(format, "Lifetime", Lifetime);
			b.AppendFormat(format, "Read", ReadConsistency);
			b.AppendFormat(format, "Write", WriteConsistency);

			return b.ToString();
		}

		/// <summary>
		/// 
		/// </summary>
		public string Keyspace { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public int Timeout { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public int PoolSize { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public int Lifetime { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public bool Pooled { get; private set; }

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
		public string ConnectionString { get; private set; }
	}
}