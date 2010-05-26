using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public class CassandraContext : IDisposable
	{
		private readonly ConnectionBuilder _connectionBuilder;
		private readonly IConnectionProvider _connectionProvider;
		private readonly IConnection _connection;
		private readonly CassandraKeyspace _keyspace;

		private IList<IFluentMutationTracker> _trackers;
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
			_trackers = new List<IFluentMutationTracker>();
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
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

		/// <summary>
		/// Gets a typed column family.
		/// </summary>
		/// <typeparam name="CompareWith"></typeparam>
		/// <returns></returns>
		public CassandraColumnFamily<CompareWith> GetColumnFamily<CompareWith>(string columnFamily)
			where CompareWith : CassandraType
		{
			return new CassandraColumnFamily<CompareWith>(this, _keyspace, _connection, columnFamily);
		}

		/// <summary>
		/// Gets a typed super column family.
		/// </summary>
		/// <typeparam name="CompareWith"></typeparam>
		/// <typeparam name="CompareSubcolumnWith"></typeparam>
		/// <param name="columnFamily"></param>
		/// <returns></returns>
		public CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> GetColumnFamily<CompareWith, CompareSubcolumnWith>(string columnFamily)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			return new CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith>(this, _keyspace, _connection, columnFamily);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void Attach(IFluentRecord record)
		{
			var tracker = record.MutationTracker;

			if (_trackers.Contains(tracker))
				return;

			_trackers.Add(tracker);
		}

		/// <summary>
		/// Saves the pending changes.
		/// </summary>
		public void SaveChanges()
		{
			var mutations = new List<FluentMutation>();

			foreach (var tracker in _trackers)
				mutations.AddRange(tracker.GetMutations());

			var op = new BatchMutate(mutations);
			ExecuteOperation(op);

			foreach (var tracker in _trackers)
				tracker.Clear();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void SaveChanges(IFluentRecord record)
		{
			var mutations = record.MutationTracker.GetMutations();
			var op = new BatchMutate(mutations);
			ExecuteOperation(op);
		}

		/// <summary>
		/// The last error that occured during the execution of an operation.
		/// </summary>
		public CassandraException LastError { get; private set; }

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(ContextOperation<TResult> action)
		{
			return ExecuteOperation<TResult>(action, false);
		}

		/// <summary>
		/// Execute the column family operation against the connection to the server.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="action"></param>
		/// <param name="throwOnError"></param>
		/// <returns></returns>
		public TResult ExecuteOperation<TResult>(ContextOperation<TResult> action, bool throwOnError)
		{
			LastError = null;

			TResult result;
			bool success = action.TryExecute(this, out result);

			if (!success)
				LastError = action.Error;

			if (!success && throwOnError)
				throw action.Error;

			return result;
		}

		#region IDisposable Members

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

		#endregion
	}
}