using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraContext : IDisposable
	{
		private static string _initHost;
		private static int _initPort;

		public static void Init(string host, int port = 9160)
		{
			_initHost = host;
			_initPort = port;
		}

		private string _keyspace;
		private string _host;
		private int _port;

		private TTransport _transport;
		private TProtocol _protocol;
		private Cassandra.Client _client;

		public CassandraContext(string keyspace)
			: this(keyspace, _initHost, _initPort) { }

		public CassandraContext(string keyspace, string host, int port = 9160)
		{
			_keyspace = keyspace;
			_host = host;
			_port = port;

			_transport = new TSocket(_host, _port);
			_protocol = new TBinaryProtocol(_transport);
			_client = new Cassandra.Client(_protocol);
		}

		public void Open()
		{
			_transport.Open();
		}

		public void Close()
		{
			_transport.Close();
		}

		public void Insert(FluentColumnFamily record)
		{
			Insert(record.ColumnFamily, record.Key, record);
		}

		public void Insert(string columnFamily, string key, FluentColumnFamily record)
		{
			var utf8 = Encoding.UTF8;

			foreach (var col in record)
			{
				var path = new ColumnPath {
					Column_family = columnFamily,
					Column = col.NameBytes
				};

				_client.insert(
					_keyspace,
					key,
					path,
					col.ValueBytes,
					col.Timestamp.Ticks,
					ConsistencyLevel.ONE
				);
			}
		}

		public void Remove(FluentColumnFamily record, string columnName = null)
		{
			Remove(record.ColumnFamily, record.Key, columnName);
		}

		public void Remove(string columnFamily, string key, string columnName = null)
		{
			var utf8 = Encoding.UTF8;
			var path = new ColumnPath {
				Column_family = columnFamily
			};

			if (!String.IsNullOrWhiteSpace(columnName))
				path.Column = FluentColumn<string>.GetBytes(columnName);

			_client.remove(
				_keyspace,
				key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel.ONE
			);
		}

		#region IDisposable Members

		public void Dispose()
		{
			if (_transport != null && _transport.IsOpen)
				_transport.Close();
		}

		#endregion
	}
}
