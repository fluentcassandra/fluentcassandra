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

		public const string TestKey1 = "Test1";
		public const string TestKey2 = "Test2";

		public const string TestStandardName = "Test1";
		public const string TestSuperName = "SubTest1";

		public _CassandraSetup()
		{
			var keyspaceName = "Testing";
			var server = new Server("localhost");

			if (!CassandraSession.KeyspaceExists(server, keyspaceName))
				CassandraSession.AddKeyspace(server, new KsDef {
					Name = "Testing",
					Replication_factor = 1,
					Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
					Cf_defs = new List<CfDef>()
				});

			var keyspace = new CassandraKeyspace(keyspaceName);

			if (!keyspace.ColumnFamilyExists(server, "Standard"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "Standard",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "AsciiType",
					Comment = "Used for testing Standard family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardAsciiType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardAsciiType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "AsciiType",
					Comment = "Used for testing Standard family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardBytesType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardBytesType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "BytesType",
					Comment = "Used for testing BytesType family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardIntegerType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardIntegerType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "IntegerType",
					Comment = "Used for testing IntegerType family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardLexicalUUIDType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardLexicalUUIDType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "LexicalUUIDType",
					Comment = "Used for testing LexicalUUIDType family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardLongType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardLongType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "LongType",
					Comment = "Used for testing LongType family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardTimeUUIDType"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardTimeUUIDType",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "TimeUUIDType",
					Comment = "Used for testing TimeUUIDType family."
				});

			if (!keyspace.ColumnFamilyExists(server, "StandardUTF8Type"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "StandardUTF8Type",
					Keyspace = "Testing",
					Column_type = "Standard",
					Comparator_type = "UTF8Type",
					Comment = "Used for testing UTF8Type family."
				});

			if (!keyspace.ColumnFamilyExists(server, "Super"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "Super",
					Keyspace = "Testing",
					Column_type = "Super",
					Comparator_type = "AsciiType",
					Subcomparator_type = "AsciiType",
					Comment = "Used for testing Super family."
				});

			DB = new CassandraContext(keyspaceName, server);
			DB.ThrowErrors = true;

			Family = DB.GetColumnFamily<AsciiType>("Standard");
			SuperFamily = DB.GetColumnFamily<AsciiType, AsciiType>("Super");

			Family.RemoveAllRows();
			SuperFamily.RemoveAllRows();

			Family.InsertColumn(TestKey1, "Test1", Math.PI);
			Family.InsertColumn(TestKey1, "Test2", Math.PI);
			Family.InsertColumn(TestKey1, "Test3", Math.PI);

			SuperFamily.InsertColumn(TestKey1, TestSuperName, "Test1", Math.PI);
			SuperFamily.InsertColumn(TestKey1, TestSuperName, "Test2", Math.PI);
			SuperFamily.InsertColumn(TestKey1, TestSuperName, "Test3", Math.PI);

			Family.InsertColumn(TestKey2, "Test1", Math.PI);
			Family.InsertColumn(TestKey2, "Test2", Math.PI);
			Family.InsertColumn(TestKey2, "Test3", Math.PI);

			SuperFamily.InsertColumn(TestKey2, TestSuperName, "Test1", Math.PI);
			SuperFamily.InsertColumn(TestKey2, TestSuperName, "Test2", Math.PI);
			SuperFamily.InsertColumn(TestKey2, TestSuperName, "Test3", Math.PI);
		}
	}
}
