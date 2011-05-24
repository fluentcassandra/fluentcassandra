using System;

namespace FluentCassandra.Connections
{
	public class Server
	{
		public const int DefaultPort = 9160;
		public const int DefaultTimeout = 0;

		public Server(string host = "127.0.0.1", int port = DefaultPort, int timeout = DefaultTimeout)
		{
			Host = host;
			Port = port;
			Timeout = timeout;
		}

		public int Port { get; private set; }

		public string Host { get; private set; }

		public int Timeout { get; private set; }

		public override string ToString()
		{
			return String.Concat(Host, ":", Port, ",", Timeout, " secs");
		}
	}
}
