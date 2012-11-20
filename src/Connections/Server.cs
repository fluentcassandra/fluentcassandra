using System;

namespace FluentCassandra.Connections
{
	public class Server : IEquatable<Server>
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

		#region IEquatable<Server> Members

		public bool Equals(Server other)
		{
			return other != null && Host == other.Host && Port == other.Port;
		}

		#endregion

		public override bool Equals(object obj)
		{
			return obj is Server && Equals((Server)obj);
		}

		public override int GetHashCode()
		{
			return Host.GetHashCode() + Port.GetHashCode();
		}
	}
}
