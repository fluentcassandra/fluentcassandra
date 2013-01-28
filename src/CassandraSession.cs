using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Apache.Cassandra;
using FluentCassandra.Connections;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public class CassandraSession : IDisposable
	{
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="server"></param>
		/// <param name="timeout"></param>
		public CassandraSession(string keyspace, Server server, string username = null, string password = null)
			: this(keyspace, server.Host, server.Port, server.Timeout, username, password) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="timeout"></param>
		/// <param name="provider"></param>
		public CassandraSession(string keyspace, string host, int port = Server.DefaultPort, int timeout = Server.DefaultTimeout, string username = null, string password = null)
			: this(new ConnectionBuilder(keyspace, host, port, timeout, username: username, password: password)) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionString"></param>
		public CassandraSession(string connectionString)
			: this(new ConnectionBuilder(connectionString)) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionBuilder"></param>
		public CassandraSession(IConnectionBuilder connectionBuilder)
			: this(ConnectionProviderFactory.Get(connectionBuilder), connectionBuilder.ReadConsistency, connectionBuilder.WriteConsistency) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionProvider"></param>
		/// <param name="read"></param>
		/// <param name="write"></param>
		public CassandraSession(IConnectionProvider connectionProvider, ConsistencyLevel read, ConsistencyLevel write)
		{
			if (connectionProvider == null)
				throw new ArgumentNullException("connectionProvider");

			ConnectionBuilder = connectionProvider.ConnectionBuilder;
			ConnectionProvider = connectionProvider;
			ReadConsistency = read;
			WriteConsistency = write;

			IsAuthenticated = false;
		}

		/// <summary>
		/// The connection builder that is currently in use for this session.
		/// </summary>
		public IConnectionBuilder ConnectionBuilder { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public IConnectionProvider ConnectionProvider { get; private set; }

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
		public bool IsAuthenticated { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="setKeyspace"></param>
		/// <returns></returns>
		internal CassandraClientWrapper GetClient(bool setKeyspace = true, bool? setCqlVersion = null, bool login = true)
		{
			var builder = ConnectionProvider.ConnectionBuilder;
			setCqlVersion = setCqlVersion ?? (builder.CqlVersion != null);

			if (_connection == null)
				_connection = ConnectionProvider.Open();

			if (!_connection.IsOpen)
				_connection.Open();

			if (setKeyspace)
				_connection.SetKeyspace(builder.Keyspace);

			if (setCqlVersion.Value)
				_connection.SetCqlVersion(builder.CqlVersion);

			if (login && !String.IsNullOrWhiteSpace(builder.Username) && !String.IsNullOrWhiteSpace(builder.Password))
				Login(builder.Username, builder.Password);

			return new CassandraClientWrapper(_connection.Client);
		}

		internal void MarkCurrentConnectionAsUnhealthy(Exception exc)
		{
			ConnectionProvider.ErrorOccurred(_connection, exc);
			_connection = null;
		}

		/// <summary>
		/// 
		/// </summary>
		public void Login()
		{
			var builder = ConnectionProvider.ConnectionBuilder;

			if (String.IsNullOrWhiteSpace(builder.Username) || String.IsNullOrWhiteSpace(builder.Password))
				throw new CassandraException("No username and/or password was set in the connection string, please use Login(username, password) method.");

			Login(builder.Username, builder.Password);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="username"></param>
		/// <param name="password"></param>
		public void Login(string username, string password)
		{
			var auth = new AuthenticationRequest {
				Credentials = new Dictionary<string, string> { { "username", username }, { "password", password } }
			};

			try
			{
				GetClient(login: false).login(auth);
				IsAuthenticated = true;
			}
			catch (Exception exc)
			{
				IsAuthenticated = false;
				throw new CassandraException("Login failed.", exc);
			}
		}

		/// <summary>
		/// The last error that occurred during the execution of an operation.
		/// </summary>
		public CassandraException LastError { get; private set; }

		/// <summary>
		/// Indicates if errors should be thrown when occurring on operation.
		/// </summary>
		public bool ThrowErrors { get; set; }

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <param name="throwOnError"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(Operation<TResult> action, bool? throwOnError = null)
		{
			if (WasDisposed)
				throw new ObjectDisposedException(GetType().FullName);

			if (!throwOnError.HasValue)
				throwOnError = ThrowErrors;

			LastError = null;
			action.Session = this;

			TResult result;
			bool success = action.TryExecute(out result);

			if (!success)
				LastError = action.Error;

			if (!success && (throwOnError ?? ThrowErrors))
				throw action.Error;

			return result;
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
		/// 
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// The dispose.
		/// </summary>
		/// <param name="disposing">
		/// The disposing.
		/// </param>
		protected virtual void Dispose(bool disposing)
		{
			if (!WasDisposed && disposing && _connection != null)
				ConnectionProvider.Close(_connection);

			WasDisposed = true;
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="CassandraSession"/> class. 
		/// </summary>
		~CassandraSession()
		{
			Dispose(false);
		}

		#endregion
	}
}