using System;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using System.Net.Sockets;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <remarks>Borrowed much of the layout from NoRM, I just couldn't resist it was very elegant in its design.</remarks>
	/// <see href="http://github.com/robconery/NoRM/tree/master/NoRM/Connections/"/>
	public class Connection : IConnection, IDisposable
	{
		private bool _disposed;

		private TTransport _transport;
		private TProtocol _protocol;
		private Cassandra.Client _client;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		internal Connection(Server server, int timeout = 0)
		{
			Created = DateTime.Now;
			Server = server;
			Timeout = timeout;

			//TcpClient client = new TcpClient(server.Host, server.Port);
			//client.NoDelay = true;
			//client.SendBufferSize = timeout;
			//client.ReceiveTimeout = timeout;

			//TTransport socket = new TSocket(client);

			TTransport socket = new TSocket(server.Host, server.Port, timeout);
			_transport = new TFramedTransport(socket);

			//_transport = socket;
			_protocol = new TBinaryProtocol(_transport);
			_client = new Cassandra.Client(_protocol);
		}

		/// <summary>
		/// 
		/// </summary>
		public DateTime Created
		{
			get;
			private set;
		}

		/// <summary>
		/// 
		/// </summary>
		public int Timeout
		{
			get;
			private set;
		}

		/// <summary>
		/// 
		/// </summary>
		public Server Server
		{
			get;
			private set;
		}

		/// <summary>
		/// 
		/// </summary>
		public bool IsOpen
		{
			get { return _transport.IsOpen; }
		}

		/// <summary>
		/// 
		/// </summary>
		public void Open()
		{
			if (IsOpen)
				return;

			_transport.Open();
		}

		/// <summary>
		/// 
		/// </summary>
		public void Close()
		{
			if (!IsOpen)
				return;

			_transport.Close();
		}

		public void SetKeyspace(string keyspace) 
		{
			Client.set_keyspace(keyspace);
		}

		/// <summary>
		/// 
		/// </summary>
		public Cassandra.Client Client
		{
			get { return _client; }
		}

		/// <summary>
		/// 
		/// </summary>
		public override string ToString()
		{
			return String.Format("{0}/{1}", Server, Created);
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}

		/// <summary>
		/// Releases unmanaged and - optionally - managed resources
		/// </summary>
		/// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
		protected virtual void Dispose(bool disposing)
		{
			if (_disposed) {
				return;
			}

			Close();
			_disposed = true;
		}

		/// <summary>
		/// Releases unmanaged resources and performs other cleanup operations before the
		/// <see cref="Connection"/> is reclaimed by garbage collection.
		/// </summary>
		~Connection()
		{
			Dispose(false);
		}
	}
}
