using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
		public ConnectionBuilder(string keyspace, string host, int port = 9160, int timeout = 0, string provider = "Normal")
		{
			Keyspace = keyspace;
			Servers = new List<Server>() { new Server(host, port) };
			Timeout = timeout;
			Provider = GetConnectionProvider(provider);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionString"></param>
		public ConnectionBuilder(string connectionString)
		{
			InitializeConnectionString(connectionString);
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

			if (!pairs.ContainsKey("Keyspace"))
				throw new CassandraException("Keyspace is required to create a connection");

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

			#region Provider

			if (!pairs.ContainsKey("Provider"))
				Provider = GetConnectionProvider("Normal");
			else
				Provider = GetConnectionProvider(pairs["Provider"]);

			#endregion
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		private IConnectionProvider GetConnectionProvider(string name)
		{
			switch (name.ToLower())
			{
				case "normal": return new NormalConnectionProvider(this);
				case "failover" : return new FailoverConnectionProvider(this);
			}

			throw new CassandraException("Cannot determine which connection provider should be used for " + name);
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
		public IList<Server> Servers { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public IConnectionProvider Provider { get; private set; }
	}
}