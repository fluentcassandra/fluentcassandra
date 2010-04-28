using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

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
		protected Cassandra.Client GetClient()
		{
			return _connection.Client;
		}

		/// <summary>
		/// Gets a typed column family.
		/// </summary>
		/// <typeparam name="T">Type of column family.</typeparam>
		/// <returns></returns>
		public CassandraColumnFamily<T> GetColumnFamily<T>(string columnFamily)
			where T : CassandraType
		{
			return new CassandraColumnFamily<T>(this, _keyspace, _connection, columnFamily);
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

			BatchMutate(mutations);

			foreach (var tracker in _trackers)
				tracker.Clear();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void SaveChanges(IFluentRecord record)
		{
			BatchMutate(record);
		}

		/*
		 * batch_mutate(keyspace, mutation_map, consistency_level)
		 */

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void BatchMutate(IFluentRecord record)
		{
			BatchMutate(record.MutationTracker.GetMutations());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void BatchMutate(IEnumerable<FluentMutation> tracker)
		{
			var mutations = new Dictionary<string, Dictionary<string, List<Mutation>>>();

			foreach (var key in tracker.GroupBy(x => x.Column.Family.Key))
			{
				var keyMutations = new Dictionary<string, List<Mutation>>();

				foreach (var columnFamily in key.GroupBy(x => x.Column.Family.FamilyName))
				{
					var columnFamilyMutations = columnFamily
						.Where(m => m.Type == MutationType.Added || m.Type == MutationType.Changed)
						.Select(m => ObjectHelper.CreateInsertedOrChangedMutation(m))
						.ToList();

					var superColumnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.SuperColumn != null);

					foreach (var superColumn in superColumnsNeedingDeleted.GroupBy(x => x.Column.SuperColumn.Name))
						columnFamilyMutations.Add(ObjectHelper.CreateDeletedSuperColumnMutation(superColumn));

					var columnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.SuperColumn == null);

					columnFamilyMutations.Add(ObjectHelper.CreateDeletedColumnMutation(columnsNeedingDeleted));

					keyMutations.Add(columnFamily.Key, columnFamilyMutations);
				}

				mutations.Add(key.Key, keyMutations);
			}

			BatchMutate(mutations);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutationMap"></param>
		protected void BatchMutate(Dictionary<string, Dictionary<string, List<Mutation>>> mutationMap)
		{
			// Dictionary<string : key, Dicationary<string : columnFamily, List<Mutation>>>
			GetClient().batch_mutate(
				Keyspace.KeyspaceName,
				mutationMap,
				ConsistencyLevel.ONE
			);
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