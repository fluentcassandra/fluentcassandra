using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class ColumnFamilyMap<T>
	{
		private string _keyspace;
		private string _columnFamily;
		private Func<T, string> _key;
		private IDictionary<string, Func<T, object>> _cache;

		public ColumnFamilyMap()
		{
			_cache = new Dictionary<string, Func<T, object>>();
		}

		protected void Keyspace(string keyspace)
		{
			_keyspace = keyspace;
		}

		protected void ColumnFamily(string columnFamily)
		{
			_columnFamily = columnFamily;
		}

		protected void Key(Expression<Func<T, string>> exp)
		{
			_key = exp.Compile();
		}

		protected void Map(Expression<Func<T, object>> exp)
		{
			Map(exp, null);
		}

		protected void Map(Expression<Func<T, object>> exp, string columnName)
		{
			// if the column name isn't present then use the member reference name
			if (String.IsNullOrWhiteSpace(columnName))
			{
				columnName = GetName(exp);

				if (columnName == null)
					throw new NotSupportedException("The expression could not be used to determine the column name automatically.");
			}

			var func = exp.Compile();
			_cache.Add(columnName, func);
		}

		private string GetName(Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.MemberAccess:
					return ((MemberExpression)exp).Member.Name;

				case ExpressionType.Convert:
				case ExpressionType.Quote:
					return GetName(((UnaryExpression)exp).Operand);

				case ExpressionType.Lambda:
					return GetName(((LambdaExpression)exp).Body);

				default:
					return null;
			}
		}

		public FluentColumnFamily PrepareColumnFamily(T obj)
		{
			FluentColumnFamily record = new FluentColumnFamily();
			record.ColumnFamily = _columnFamily;
			record.Key = _key(obj);

			foreach (var col in _cache)
			{
				record.Columns.Add(new FluentColumn<string> {
					Name = col.Key,
					Value = col.Value(obj)
				});
			}

			return record;
		}

		public void Insert(T obj)
		{
			Insert(PrepareColumnFamily(obj));
		}

		public void Insert(FluentColumnFamily record)
		{
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client client = new Cassandra.Client(protocol);

			transport.Open();

			var utf8 = Encoding.UTF8;
			string keySpace = _keyspace;
			string columnFamily = record.ColumnFamily;
			string key = record.Key;

			foreach (var col in record)
			{
				var path = new ColumnPath {
					Column_family = columnFamily,
					Column = col.NameBytes
				};

				client.insert(
					keySpace,
					key,
					path,
					col.ValueBytes,
					col.Timestamp.Ticks,
					ConsistencyLevel.ONE
				);
			}

			transport.Close();
		}

		public void Remove(T obj)
		{
			RemoveByKey(_key(obj));
		}

		public void RemoveByKey(string key)
		{
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client client = new Cassandra.Client(protocol);

			transport.Open();

			var utf8 = Encoding.UTF8;
			string keySpace = _keyspace;
			string columnFamily = _columnFamily;

			var path = new ColumnPath {
				Column_family = columnFamily
			};

			client.remove(
				keySpace,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);

			transport.Close();
		}

		public void RemoveColumn(string key, string columnName)
		{
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client client = new Cassandra.Client(protocol);

			transport.Open();

			var utf8 = Encoding.UTF8;
			string keySpace = _keyspace;
			string columnFamily = _columnFamily;

			var path = new ColumnPath {
				Column_family = columnFamily,
				Column = FluentColumn<string>.GetBytes(columnName)
			};

			client.remove(
				keySpace,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);

			transport.Close();
		}
	}
}