using System;
using System.Collections.Generic;
using FluentCassandra.Linq;
using FluentCassandra.Types;
using FluentCassandra.Operations;
using FluentCassandra.Connections;

namespace FluentCassandra
{
	public class CassandraContext : IDisposable
	{
		[ThreadStatic]
		private static ConnectionBuilder _currentConnectionBuilder;

		public static ConnectionBuilder CurrentConnectionBuilder
		{
			get { return _currentConnectionBuilder; }
			internal set { _currentConnectionBuilder = value; }
		}

		private IList<IFluentMutationTracker> _trackers;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="server"></param>
		/// <param name="timeout"></param>
		public CassandraContext(string keyspace, Server server, string username = null, string password = null)
			: this(keyspace, server.Host, server.Port, server.Timeout, username, password) { }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="timeout"></param>
		/// <param name="provider"></param>
		public CassandraContext(string keyspace, string host, int port = Server.DefaultPort, int timeout = Server.DefaultTimeout, string username = null, string password = null)
			: this(new ConnectionBuilder(keyspace, host, port, timeout, username: username, password: password)) { }

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
			CurrentConnectionBuilder = connectionBuilder;
			ThrowErrors = true;
			
			_trackers = new List<IFluentMutationTracker>();
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
			lock (_trackers)
			{
				var mutations = new List<FluentMutation>();

				foreach (var tracker in _trackers)
					mutations.AddRange(tracker.GetMutations());

				var op = new BatchMutate(mutations);
				ExecuteOperation(op);

				foreach (var tracker in _trackers)
					tracker.Clear();

				_trackers.Clear();
			}
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
		/// 
		/// </summary>
		/// <param name="cqlQuery"></param>
		public IEnumerable<ICqlRow<BytesType>> ExecuteQuery(UTF8Type cqlQuery)
		{
			var op = new ExecuteCqlQuery<BytesType>(cqlQuery);
			return ExecuteOperation(op);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="cqlQuery"></param>
		public void ExecuteNonQuery(UTF8Type cqlQuery)
		{
			var op = new ExecuteCqlNonQuery(cqlQuery);
			ExecuteOperation(op);
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
			if (!throwOnError.HasValue)
				throwOnError = ThrowErrors;

			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				LastError = null;
	
				TResult result;
				bool success = action.TryExecute(out result);

				if (!success)
					LastError = action.Error;

				if (!success && (throwOnError ?? ThrowErrors))
					throw action.Error;

				return result;
			}
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		#region IDisposable Members

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
			if (!WasDisposed && disposing && CassandraSession.Current != null)
			{
				CassandraSession.Current.Dispose();
				CassandraSession.Current = null;
			}

			WasDisposed = true;
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="Mongo"/> class. 
		/// </summary>
		~CassandraContext()
		{
			Dispose(false);
		}

		#endregion
	}
}