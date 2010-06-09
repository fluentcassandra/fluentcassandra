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
		private IConnection _connection;

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
		public CassandraContext(string keyspace, string host, int port = 9160, int timeout = 0)
			: this(new ConnectionBuilder(keyspace, host, port, timeout)) { }

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
			: this(connectionBuilder.Keyspace, connectionBuilder) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connectionBuilder"></param>
		public CassandraContext(string keyspace, ConnectionBuilder connectionBuilder)
			: this(keyspace, ConnectionProviderFactory.Get(connectionBuilder), connectionBuilder.ReadConsistency, connectionBuilder.WriteConsistency) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connectionBuilder"></param>
		public CassandraContext(string keyspace, IConnectionProvider connectionProvider, ConsistencyLevel read, ConsistencyLevel write)
		{
			if (String.IsNullOrEmpty(keyspace))
				throw new ArgumentNullException("keyspace");

			ConnectionProvider = connectionProvider;
			Keyspace = new CassandraKeyspace(keyspace, _connection);
			ReadConsistency = read;
			WriteConsistency = write;

			_trackers = new List<IFluentMutationTracker>();
		}

		/// <summary>
		/// Gets the database.
		/// </summary>
		public CassandraKeyspace Keyspace { get; private set; }

		/// <summary>
		/// Gets ConnectionProvider.
		/// </summary>
		public IConnectionProvider ConnectionProvider { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		internal ConsistencyLevel ReadConsistency { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		internal ConsistencyLevel WriteConsistency { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		internal Cassandra.Client GetClient()
		{
			if (_connection == null || !_connection.IsOpen)
				_connection = ConnectionProvider.Open();

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
			return new CassandraColumnFamily<CompareWith>(this, columnFamily);
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
			return new CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith>(this, columnFamily);
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
				this.ConnectionProvider.Close(this._connection);

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