using System;
using FluentCassandra.Thrift.Transport;
using FluentCassandra.Thrift.Protocol;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Connections
{
	/// <summary>
	/// 
	/// </summary>
	/// <remarks>Borrowed much of the layout from NoRM, I just couldn't resist it was very elegant in its design.</remarks>
	/// <see href="http://github.com/robconery/NoRM/tree/master/NoRM/Connections/"/>
	public class Connection : IConnection, IDisposable
	{
		private TTransport _transport;
		private Cassandra.Client _client;
		private string _activeKeyspace;
		private string _activeCqlVersion;
		private readonly object _lock = new object();

		/// <summary>
		/// 
		/// </summary>
		/// <param name="server"></param>
		/// <param name="builder"></param>
		public Connection(Server server, IConnectionBuilder builder)
			: this(server, builder.ConnectionType, builder.BufferSize) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="server"></param>
		/// <param name="connectionType"></param>
		/// <param name="bufferSize"></param>
		public Connection(Server server, ConnectionType connectionType, int bufferSize)
		{
			Created = DateTime.UtcNow;
			Server = server;
			ConnectionType = connectionType;
			BufferSize = bufferSize;
			InitTransportAndClient();
		}

		/// <summary>
		/// 
		/// </summary>
		private void InitTransportAndClient()
		{
            var socket = new TSocketWithConnectTimeout(Server.Host, Server.Port, Server.Timeout * 1000);

			switch (ConnectionType)
			{
				case ConnectionType.Simple:
					_transport = socket;
					break;

				case ConnectionType.Buffered:
					_transport = new TBufferedTransport(socket, BufferSize);
					break;

				case ConnectionType.Framed:
					_transport = new TFramedTransport(socket);
					break;

				default:
					goto case ConnectionType.Framed;
			}

			var protocol = new TBinaryProtocol(_transport);
			_client = new Cassandra.Client(protocol);
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
		public ConnectionType ConnectionType
		{
			get;
			private set;
		}

		/// <summary>
		/// 
		/// </summary>
		public int BufferSize
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

				lock (_lock)
					return _transport.IsOpen;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public void Open()
		{
			CheckWasDisposed();

			if (IsOpen)
				return;

			if (_transport == null)
				InitTransportAndClient();

			lock (_lock)
				_transport.Open();
		}

		/// <summary>
		/// 
		/// </summary>
		public void Close()
		{
			CheckWasDisposed();

			if (!IsOpen)
				return;

			lock (_lock)
			{
				_transport.Close();
				_transport = null;
				_client = null;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		public void SetKeyspace(string keyspace)
		{
			CheckWasDisposed();

			if (!IsOpen)
				throw new CassandraConnectionException("A connection to Cassandra has not been opened.");

			if (_activeKeyspace == null || !_activeKeyspace.Equals(keyspace))
			{
				Client.set_keyspace(keyspace);
				_activeKeyspace = keyspace;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cqlVersion"></param>
		[Obsolete("This will be retired soon, please pass the CQL version through the Execute method.", error: false)]
		public void SetCqlVersion(string cqlVersion)
		{
			CheckWasDisposed();

			if (!IsOpen)
				throw new CassandraConnectionException("A connection to Cassandra has not been opened.");

			if (_activeCqlVersion == null || !_activeCqlVersion.Equals(cqlVersion))
			{
				Client.set_cql_version(cqlVersion);
				_activeCqlVersion = cqlVersion;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public Cassandra.Client Client
		{
			get
			{
				lock(_lock)
					return _client;
			}
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
		private void CheckWasDisposed()
		{
			if (WasDisposed)
				throw new ObjectDisposedException("connection has been disposed of");
		}

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