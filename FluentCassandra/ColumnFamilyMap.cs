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
	public class ColumnFamilyMap<T>
	{
		private string _keyspace;
		private ITypeGetConfiguration<T> _config;

		public ColumnFamilyMap()
		{
			_config = CassandraConfiguration.GetMapFor<T>();
		}

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

		public void Insert(T obj)
		{
			var record = PrepareColumnFamily(obj);
			CassandraDatabase context = new CassandraDatabase();
			context.Open();
			context.Insert(record);
			context.Close();
		}

		public void Remove(T obj)
		{
			CassandraDatabase context = new CassandraDatabase();
			context.Open();
			context.Remove(_config.ColumnFamily, _config.KeyMap.KeyAccessor(obj));
			context.Close();
		}
	}
}