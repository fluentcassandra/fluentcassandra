using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using System.Collections;

namespace FluentCassandra
{
	public class CassandraColumnFamily
	{
		private CassandraKeyspace _keyspace;
		private IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraKeyspace keyspace, IConnection connection)
		{
			_keyspace = keyspace;
			_connection = connection;
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
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="superColumnName"></param>
		/// <returns></returns>
		public int ColumnCount(FluentColumnFamily record, string superColumnName = null)
		{
			return ColumnCount(record.Key, record.ColumnFamily, superColumnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="superColumnName"></param>
		/// <returns></returns>
		public int ColumnCount(string key, string columnFamily, string superColumnName)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				parent.Super_column = superColumnName.GetBytes();

			return GetClient().get_count(
				_keyspace.KeyspaceName,
				key,
				parent,
				ConsistencyLevel.ONE
			);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		public void Insert(FluentColumnFamily record)
		{
			Insert(record.Key, record.ColumnFamily, record);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="record"></param>
		public void Insert(string key, string columnFamily, IEnumerable<FluentColumnPath<string>> record)
		{
			foreach (var col in record)
			{
				var path = new ColumnPath {
					Column_family = col.ColumnFamily.ColumnFamily,
					Column = col.Column.NameBytes
				};

				if (col.SuperColumn != null)
					path.Super_column = col.SuperColumn.NameBytes;

				GetClient().insert(
					_keyspace.KeyspaceName,
					key,
					path,
					col.Column.ValueBytes,
					col.Column.Timestamp.Ticks,
					ConsistencyLevel.ONE
				);
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		/// <param name="superColumnName"></param>
		public void Remove(FluentColumnFamily record, string superColumnName = null, string columnName = null)
		{
			Remove(record.Key, record.ColumnFamily, superColumnName, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="key"></param>
		/// <param name="columnName"></param>
		public void Remove(string key, string columnFamily, string superColumnName, string columnName)
		{
			var path = new ColumnPath {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(superColumnName))
				path.Super_column = superColumnName.GetBytes();

			if (!String.IsNullOrWhiteSpace(columnName))
				path.Column = columnName.GetBytes();

			GetClient().remove(
				_keyspace.KeyspaceName,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);
		}
	}
}
