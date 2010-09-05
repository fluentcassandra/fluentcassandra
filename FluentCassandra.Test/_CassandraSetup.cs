using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Test
{
	internal class _CassandraSetup
	{
		public CassandraContext DB;
		public CassandraColumnFamily<AsciiType> Family;
		public CassandraSuperColumnFamily<AsciiType, AsciiType> SuperFamily;

		public const string TestKey = "Test1";
		public const string TestStandardName = "Test1";
		public const string TestSuperName = "SubTest1";

		public _CassandraSetup()
		{
			var keyspace = "Testing";
			var server = new Server("localhost");

			var ksDef = new KsDef {
				Name = "Testing",
				Replication_factor = 1,
				Strategy_class = "org.apache.cassandra.locator.RackUnawareStrategy",
				Cf_defs = new List<CfDef>()
			};

			ksDef.Cf_defs.Add(new CfDef {
				Name = TestStandardName,
				Keyspace = "Testing",
				Column_type = "Standard",
				Comparator_type = "AsciiType",
				Comment = "Used for testing Standard family."
			});

			ksDef.Cf_defs.Add(new CfDef {
				Name = TestSuperName,
				Keyspace = "Testing",
				Column_type = "Super",
				Comparator_type = "AsciiType",
				Subcomparator_type = "AsciiType",
				Comment = "Used for testing Super family."
			});

			CassandraSession.AddKeyspace(server, ksDef);

			DB = new CassandraContext(keyspace, server);
			Family = DB.GetColumnFamily<AsciiType>("Standard");
			SuperFamily = DB.GetColumnFamily<AsciiType, AsciiType>("Super");

			Family.InsertColumn(TestKey, "Test1", Math.PI);
			Family.InsertColumn(TestKey, "Test2", Math.PI);
			Family.InsertColumn(TestKey, "Test3", Math.PI);

			SuperFamily.InsertColumn(TestKey, TestSuperName, "Test1", Math.PI);
			SuperFamily.InsertColumn(TestKey, TestSuperName, "Test2", Math.PI);
			SuperFamily.InsertColumn(TestKey, TestSuperName, "Test3", Math.PI);
		}
	}
}
