using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraKeyspace
	{
		private readonly string _keyspaceName;
		private readonly IConnection _connection;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="connecton"></param>
		public CassandraKeyspace(string keyspaceName, IConnection connecton)
		{
			_keyspaceName = keyspaceName;
			_connection = connecton;
		}

		/// <summary>
		/// 
		/// </summary>
		public IConnection Connection
		{
			get { return _connection; }
		}

		/// <summary>
		/// 
		/// </summary>
		public string KeyspaceName
		{
			get { return _keyspaceName; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public CassandraColumnFamily GetColumnFamily()
		{
			return new CassandraColumnFamily(this, _connection);
		}

		/// <summary>
		/// Gets a typed column family.
		/// </summary>
		/// <typeparam name="T">Type of column family.</typeparam>
		/// <returns></returns>
		public CassandraColumnFamily<T> GetColumnFamily<T>()
		{
			return new CassandraColumnFamily<T>(this, _connection);
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

				_connection.Client.insert(
					_keyspaceName,
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

			_connection.Client.remove(
				_keyspaceName,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);
		}
	}
}
