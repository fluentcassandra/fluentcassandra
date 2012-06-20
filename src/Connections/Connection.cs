using System;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra.Connections
{
	/// <summary>
	/// 
	/// </summary>
	/// <remarks>Borrowed much of the layout from NoRM, I just couldn't resist it was very elegant in its design.</remarks>
	/// <see href="http://github.com/robconery/NoRM/tree/master/NoRM/Connections/"/>
	public class Connection : IConnection, IDisposable
	{
		private readonly ConnectionType _connectionType;
		private readonly int _bufferSize;

		private readonly TTransport _transport;
		private readonly TProtocol _protocol;
		private readonly Cassandra.Client _client;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		internal Connection(Server server, IConnectionBuilder builder)
		{
			_connectionType = builder.ConnectionType;
			_bufferSize = builder.BufferSize;

			Created = DateTime.UtcNow;
			Server = server;

			var socket = new TSocket(server.Host, server.Port, server.Timeout * 1000);

			switch (_connectionType)
			{
				case ConnectionType.Simple:
					_transport = socket;
					break;

				case ConnectionType.Buffered:
					_transport = new TBufferedTransport(socket, _bufferSize);
					break;

				case ConnectionType.Framed:
					_transport = new TFramedTransport(socket);
					break;

				default:
					goto case ConnectionType.Framed;
			}

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
			get
			{
				if (_transport == null)
					return false;

				lock (_transport)
				{
					try { return _transport.IsOpen; }
					catch { return false; }
				}
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public void Open()
		{
			if (IsOpen)
				return;

			lock (_transport)
				_transport.Open();
		}

		/// <summary>
		/// 
		/// </summary>
		public void Close()
		{
			if (!IsOpen)
				return;

			lock (_transport)
				_transport.Close();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		public void SetKeyspace(string keyspace)
		{
			Client.set_keyspace(keyspace);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cqlVersion"></param>
		public void SetCqlVersion(string cqlVersion)
		{
			Client.set_cql_version(cqlVersion);
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

		#region IDisposable Members

		/// <summary>
		/// 
		/// </summary>
		public bool WasDisposed
		{
			get;
			private set;
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Releases unmanaged and - optionally - managed resources
		/// </summary>
		/// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
		protected virtual void Dispose(bool disposing)
		{
			if (!WasDisposed && disposing && _transport != null)
			{
				Close();

				_client = null;
				_protocol = null;
				_transport = null;
			}

			WasDisposed = true;
		}

		/// <summary>
		/// Releases unmanaged resources and performs other cleanup operations before the
		/// <see cref="Connection"/> is reclaimed by garbage collection.
		/// </summary>
		~Connection()
		{
			Dispose(false);
		}

		#endregion
	}
}