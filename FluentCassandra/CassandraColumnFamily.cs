using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

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
		public void Insert(FluentColumnFamily record)
		{
			Insert(record.ColumnFamily, record.Key, record);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="key"></param>
		/// <param name="record"></param>
		public void Insert(string columnFamily, string key, FluentColumnFamily record)
		{
			var utf8 = Encoding.UTF8;

			foreach (var col in record)
			{
				var path = new ColumnPath {
					Column_family = columnFamily,
					Column = col.NameBytes
				};

				GetClient().insert(
					_keyspace.KeyspaceName,
					key,
					path,
					col.ValueBytes,
					col.Timestamp.Ticks,
					ConsistencyLevel.ONE
				);
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="record"></param>
		/// <param name="columnName"></param>
		public void Remove(FluentColumnFamily record, string columnName = null)
		{
			Remove(record.ColumnFamily, record.Key, columnName);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="key"></param>
		/// <param name="columnName"></param>
		public void Remove(string columnFamily, string key, string columnName = null)
		{
			var utf8 = Encoding.UTF8;
			var path = new ColumnPath {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(columnName))
				path.Column = FluentColumn<string>.GetBytes(columnName);

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
