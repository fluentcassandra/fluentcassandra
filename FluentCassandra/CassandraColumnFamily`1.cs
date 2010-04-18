using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using FluentCassandra.Configuration;

namespace FluentCassandra
{
	public class CassandraColumnFamily<T> : CassandraColumnFamily
	{
		private ITypeGetConfiguration<T> _config;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspace"></param>
		/// <param name="connection"></param>
		public CassandraColumnFamily(CassandraKeyspace keyspace, IConnection connection)
			: base(keyspace, connection)
		{
			_config = CassandraConfiguration.GetMapFor<T>();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		/// <returns></returns>
		public FluentColumnFamily PrepareColumnFamily(T obj)
		{
			FluentColumnFamily record = new FluentColumnFamily();
			record.ColumnFamily = _config.ColumnFamily;
			record.Key = _config.KeyMap.KeyAccessor(obj);

			foreach (var col in _config.PropertyMap)
			{
				record.Columns.Add(new FluentColumn<string> {
					Name = col.Alias,
					Value = col.PropertyAccessor(obj)
				});
			}

			return record;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		public void Insert(T obj)
		{
			Insert(PrepareColumnFamily(obj));
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		public void Remove(T obj)
		{
			Remove(PrepareColumnFamily(obj));
		}
	}
}