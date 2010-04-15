using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using FluentCassandra.Configuration;

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

	public class LocationMap : ColumnFamilyMap<Location> { }

	internal class Program
	{
		private static void Main(string[] args)
		{
			CassandraDatabase.Init("Keyspace1");

			CassandraConfiguration.Initialize(c => {
				c.For<Location>(m => {
					m.UseColumnFamily("Location");
					m.ForKey(p => p.Id);
					m.ForProperty(p => p.Latitude);
					m.ForProperty(p => p.Longitude);
					m.ForProperty(p => p.Street);
					m.ForProperty(p => p.City);
					m.ForProperty(p => p.State);
					m.ForProperty(p => p.PostalCode);
					m.ForProperty(p => p.Name);
				});
			});

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

			Console.WriteLine("Insert record");
			mapping.Insert(location);

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
