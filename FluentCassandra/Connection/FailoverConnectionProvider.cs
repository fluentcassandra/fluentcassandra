using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;

namespace FluentCassandra
{
	public class FailoverConnectionProvider : ConnectionProvider
	{
		private int _currentIndex;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		public FailoverConnectionProvider(ConnectionBuilder builder)
			: base(builder)
		{
			if (builder.Timeout == 0)
				throw new CassandraException("You must specify a timeout for the failover provider.");

			Reset();
		}

		public Exception LastError
		{
			get;
			private set;
		}

		public void Reset()
		{
			_currentIndex = -1;
			LastError = null;
		}

		public bool HasNext
		{
			get { return _currentIndex < Builder.Servers.Count; }
		}

		public IConnection GetNextConnection()
		{
			_currentIndex++;
			int maxServers = Builder.Servers.Count;

			if (maxServers == 0)
				throw new CassandraException("No connection could be made because no servers were defined.");

			if (_currentIndex > (maxServers - 1))
				return null;

			var server = Builder.Servers[_currentIndex];
			var conn = new Connection(server, Builder.Timeout);
			return conn;
		}

		public override IConnection Open()
		{
			IConnection conn = null;

			while (HasNext)
			{
				try
				{
					conn = GetNextConnection();
					conn.Open();
					break;
				}
				catch (SocketException exc)
				{
					LastError = exc;
				}
			}

			if (conn == null)
				throw new CassandraException("No connection could be made because all servers have failed.");

			Reset();
			return conn;
		}

		public override IConnection CreateNewConnection()
		{
			return GetNextConnection();
		}
	}
}
