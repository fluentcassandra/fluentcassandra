using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			dynamic location = new FluentColumnFamily {
				Key = "19001",
				ColumnFamily = "Location"
			};

			location.Latitude = 30.0M;
			location.Longitude = -40.0M;

			location.Street = "123 Some St.";
			location.City = "Philadelphia";
			location.State = "PA";
			location.PostalCode = "19001";

			location.Name = "Some Location";

			Console.WriteLine("----------------------");
			foreach (var col in location)
				Console.WriteLine(col.ToString());

			Console.WriteLine("----------------------");
			TTransport transport = new TSocket("localhost", 9160);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client client = new Cassandra.Client(protocol);

			transport.Open();

			var utf8 = Encoding.UTF8;
			string keySpace = "Keyspace1";
			string key = ((FluentColumnFamily)location).Key;
			string columnFamily = ((FluentColumnFamily)location).ColumnFamily;

			foreach (var col in (FluentColumnFamily)location)
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

				Console.WriteLine("Inserted " + col.Name);

				var column = client.get(
					keySpace,
					key,
					path,
					ConsistencyLevel.ONE
				);

				Console.WriteLine(
					"{0}: {1} - {2}",
					utf8.GetString(column.Column.Name),
					utf8.GetString(column.Column.Value),
					column.Column.Timestamp
				);
			}

			transport.Close();

			Console.Read();
		}
	}
}
