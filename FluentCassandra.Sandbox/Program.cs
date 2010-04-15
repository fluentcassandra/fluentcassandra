using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra.Sandbox
{
	public class Location
	{
		public string Id;

		public decimal Latitude;
		public decimal Longitude;
		public string Street;
		public string City;
		public string State;
		public string PostalCode;
		public string Name;
	}

	public class LocationMap : ColumnFamilyMap<Location>
	{
		public LocationMap()
		{
			Keyspace("Keyspace1");
			ColumnFamily("Location");
			Key(x => x.Id);
			Map(x => x.Latitude);
			Map(x => x.Longitude);
			Map(x => x.Street);
			Map(x => x.City);
			Map(x => x.State);
			Map(x => x.PostalCode);
			Map(x => x.Name);
		}
	}

	internal class Program
	{
		private static void Main(string[] args)
		{
			CassandraContext.Init("localhost");

			var mapping = new LocationMap();

			var location = new Location {
				Id = "19001",
				Name = "Some Location",

				Latitude = 30.0M,
				Longitude = -40.0M,

				Street = "123 Some St.",
				City = "Philadelphia",
				State = "PA",
				PostalCode = "19001"
			};

			mapping.Insert(location);

			Console.Read();
		}
	}
}
