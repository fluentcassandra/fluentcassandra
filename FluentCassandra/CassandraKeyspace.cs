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
		public CassandraColumnFamily GetColumnFamily(string columnFamily)
		{
			return new CassandraColumnFamily(this, _connection, columnFamily);
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
	}
}
