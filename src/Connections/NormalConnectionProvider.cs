using System;
using System.Net.Sockets;

namespace FluentCassandra.Connections
{
	public class NormalConnectionProvider : ConnectionProvider
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		public NormalConnectionProvider(ConnectionBuilder builder)
			: base(builder)
		{
			if (builder.Servers.Count > 1 && builder.ConnectionTimeout == TimeSpan.Zero)
				throw new CassandraException("You must specify a timeout when using multiple servers.");

			ConnectionTimeout = builder.ConnectionTimeout;
		}

		/// <summary>
		/// 
		/// </summary>
		public TimeSpan ConnectionTimeout { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override IConnection Open()
		{
			IConnection conn = null;

			while (Servers.HasNext)
			{
				try
				{
					conn = CreateConnection();
					conn.Open();
					break;
				}
				catch (SocketException exc)
				{
					Servers.ErrorOccurred(conn.Server, exc);
					Close(conn);
					conn = null;
				}
			}

			if (conn == null)
				throw new CassandraException("No connection could be made because all servers have failed.");

			return conn;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override IConnection CreateConnection()
		{
			if (!Servers.HasNext)
				return null;

			var server = Servers.Next();
			var conn = new Connection(server, Builder);

			return conn;
		}
	}
}
