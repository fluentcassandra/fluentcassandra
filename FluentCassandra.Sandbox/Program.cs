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
				var city = table.Get("19001", null, "City");

				Console.WriteLine("Get city back");
				Console.WriteLine("City: {0}", city);
			}

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
