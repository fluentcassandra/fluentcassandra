using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Connections;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public class CassandraSession : IDisposable
	{
		[ThreadStatic]
		private static CassandraSession _current;

		public static CassandraSession Current
		{
			get { return _current; }
			internal set { _current = value; }
		}

		private IConnection _connection;

		public CassandraSession(ConnectionBuilder connectionBuilder)
			: this(ConnectionProviderFactory.Get(connectionBuilder), connectionBuilder.ReadConsistency, connectionBuilder.WriteConsistency) { }

		public CassandraSession(ConnectionBuilder connectionBuilder, ConsistencyLevel read, ConsistencyLevel write)
			: this(ConnectionProviderFactory.Get(connectionBuilder), read, write) { }

		public CassandraSession(IConnectionProvider connectionProvider, ConsistencyLevel read, ConsistencyLevel write)
		{
			if (Current != null)
				throw new CassandraException("Cannot create a new session while there is one already active.");

			ConnectionProvider = connectionProvider;
			ReadConsistency = read;
			WriteConsistency = write;

			IsAuthenticated = false;
			Current = this;
		}

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
		internal CassandraClientWrapper GetClient(bool setKeyspace = true)
		{
			if (_connection == null)
				_connection = ConnectionProvider.Open();

			if (!_connection.IsOpen)
				_connection.Open();

			if (setKeyspace)
				_connection.SetKeyspace(ConnectionProvider.Builder.Keyspace);

			var builder = ConnectionProvider.Builder;
			if (!String.IsNullOrWhiteSpace(builder.Username) && !String.IsNullOrWhiteSpace(builder.Password))
				Login(builder.Username, builder.Password);

			return new CassandraClientWrapper(_connection.Client);
		}

		/// <summary>
		/// 
		/// </summary>
		public void Login()
		{
			var builder = ConnectionProvider.Builder;

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
				GetClient().login(auth);
				IsAuthenticated = true;
			}
			catch (Exception exc)
			{
				IsAuthenticated = false;
				throw new CassandraException("Login failed.", exc);
			}
		}

		/// <summary>
		/// The last error that occured during the execution of an operation.
		/// </summary>
		public CassandraException LastError { get; private set; }

		/// <summary>
		/// Indicates if errors should be thrown when occuring on opperation.
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
			if (!WasDisposed && disposing)
			{
				if (_connection != null)
					ConnectionProvider.Close(_connection);

				if (Current == this)
					Current = null;
			}

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
