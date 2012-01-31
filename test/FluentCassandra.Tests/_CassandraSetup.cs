using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;

namespace FluentCassandra
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

			DB = new CassandraContext(keyspaceName, server);
			DB.ThrowErrors = true;

			if (DB.KeyspaceExists(keyspaceName))
				DB.DropKeyspace(keyspaceName);

			var keyspace = DB.Keyspace;
			keyspace.TryCreateSelf(server);
			keyspace.TryCreateColumnFamily<AsciiType>("Standard");
			keyspace.TryCreateColumnFamily<AsciiType, AsciiType>("Super");
			keyspace.TryCreateColumnFamily<AsciiType>("StandardAsciiType");
			keyspace.TryCreateColumnFamily<BytesType>("StandardBytesType");
			keyspace.TryCreateColumnFamily<IntegerType>("StandardIntegerType");
			keyspace.TryCreateColumnFamily<LexicalUUIDType>("StandardLexicalUUIDType");
			keyspace.TryCreateColumnFamily<LongType>("StandardLongType");
			keyspace.TryCreateColumnFamily<TimeUUIDType>("StandardTimeUUIDType");
			keyspace.TryCreateColumnFamily<UTF8Type>("StandardUTF8Type");
			keyspace.TryCreateColumnFamily<UUIDType>("StandardUUIDType");
			keyspace.TryCreateColumnFamily<CompositeType<LongType, UTF8Type>>("StandardCompositeType");
			keyspace.TryCreateColumnFamily<DynamicCompositeType>("StandardDynamicCompositeType");

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
