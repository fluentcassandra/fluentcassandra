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

	internal class Program
	{
		private static void Main(string[] args)
		{
			CassandraConfiguration.Initialize(config => {
				config.For<Location>(x => {
					x.UseColumnFamily("Location");
					x.ForKey(p => p.Id);
					x.ForProperty(p => p.Latitude);
					x.ForProperty(p => p.Longitude);
					x.ForProperty(p => p.Street);
					x.ForProperty(p => p.City);
					x.ForProperty(p => p.State);
					x.ForProperty(p => p.PostalCode);
					x.ForProperty(p => p.Name);
				});
			});

			using (var db = new CassandraContext("Keyspace1", "localhost"))
			{
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

				var table = db.GetColumnFamily<Location>();

				Console.WriteLine("Insert record");
				table.Insert(location);
			}

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
