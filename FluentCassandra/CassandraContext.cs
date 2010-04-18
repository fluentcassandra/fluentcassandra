using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class CassandraContext : IDisposable
	{
		private readonly ConnectionBuilder _connectionBuilder;
		private readonly IConnectionProvider _connectionProvider;
		private readonly IConnection _connection;
		private readonly CassandraKeyspace _keyspace;
		private bool _disposed;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="timeout"></param>
		/// <param name="provider"></param>
		public CassandraContext(string keyspace, string host, int port = 9160, int timeout = 0, string provider = "Normal")
			: this(new ConnectionBuilder(keyspace, host, port, timeout, provider)) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionString"></param>
		public CassandraContext(string connectionString)
			: this(new ConnectionBuilder(connectionString)) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connectionBuilder"></param>
		public CassandraContext(ConnectionBuilder connectionBuilder)
		{
			_connectionBuilder = connectionBuilder;
			_connectionProvider = connectionBuilder.Provider;
			_connection = _connectionProvider.Open();
			_keyspace = new CassandraKeyspace(_connectionBuilder.Keyspace, _connection);
		}

		/// <summary>
		/// Gets the database.
		/// </summary>
		public CassandraKeyspace Keyspace
		{
			get { return this._keyspace; }
		}

		/// <summary>
		/// Gets ConnectionProvider.
		/// </summary>
		public IConnectionProvider ConnectionProvider
		{
			get { return this._connectionProvider; }
		}

		/// <summary>
		/// 
		/// </summary>
		public IConnection Connection
		{
			get { return this._connection; }
		}

		/// <summary>
		/// Gets a typed column family.
		/// </summary>
		/// <typeparam name="T">Type of column family.</typeparam>
		/// <returns></returns>
		public CassandraColumnFamily<T> GetColumnFamily<T>()
		{
			return _keyspace.GetColumnFamily<T>();
		}

		/// <summary>
		/// 
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}

		/// <summary>
		/// The dispose.
		/// </summary>
		/// <param name="disposing">
		/// The disposing.
		/// </param>
		protected virtual void Dispose(bool disposing)
		{
			if (!this._disposed && disposing && this._connection != null)
				this._connectionProvider.Close(this._connection);

			this._disposed = true;
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="Mongo"/> class. 
		/// </summary>
		~CassandraContext()
		{
			this.Dispose(false);
		}
	}
}