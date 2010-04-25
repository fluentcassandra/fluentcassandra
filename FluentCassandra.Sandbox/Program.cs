using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;

using FluentCassandra.Configuration;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			using (var db = new CassandraContext("Keyspace1", "localhost"))
			{
				dynamic location = new FluentColumnFamily("19001", "Standard1");
				location.Name = "Some Location";
				location.Latitude = 30.0M;
				location.Longitude = -40.0M;
				location.Street = "123 Some St.";
				location.City = "Philadelphia";
				location.State = "PA";
				location.PostalCode = "19001";

				Console.WriteLine("attaching record");
				db.Attach(location);

				Console.WriteLine("saving changes");
				db.SaveChanges();

				var table = db.GetColumnFamily("Standard1");

				string[] columns = new[] { "Name", "Street", "City", "State", "PostalCode", "Latitude", "Longitude" };
				dynamic sameLocation = table.GetSingle("19001", columns);

				sameLocation.Hint("Latitude", typeof(decimal));
				sameLocation.Hint("Longitude", typeof(decimal));

				Console.WriteLine("Get back");
				Console.WriteLine("Name: {0}", sameLocation.Name);
				Console.WriteLine("Street: {0}", sameLocation.Street);
				Console.WriteLine("City: {0}", sameLocation.City);
				Console.WriteLine("State: {0}", sameLocation.State);
				Console.WriteLine("PostalCode: {0}", sameLocation.PostalCode);
				Console.WriteLine("Latitude: {0}", sameLocation.Latitude);
				Console.WriteLine("Longitude: {0}", sameLocation.Longitude);
			}

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
