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

			if (CassandraSession.KeyspaceExists(server, keyspaceName))
				CassandraSession.DropKeyspace(server, keyspaceName);

			var keyspace = new CassandraKeyspace(keyspaceName);
			keyspace.TryCreateSelf(server);
			keyspace.TryCreateColumnFamily<AsciiType>(server, "Standard");
			keyspace.TryCreateColumnFamily<AsciiType>(server, "StandardAsciiType");
			keyspace.TryCreateColumnFamily<BytesType>(server, "StandardBytesType");
			keyspace.TryCreateColumnFamily<IntegerType>(server, "StandardIntegerType");
			keyspace.TryCreateColumnFamily<LexicalUUIDType>(server, "StandardLexicalUUIDType");
			keyspace.TryCreateColumnFamily<LongType>(server, "StandardLongType");
			keyspace.TryCreateColumnFamily<TimeUUIDType>(server, "StandardTimeUUIDType");
			keyspace.TryCreateColumnFamily<UTF8Type>(server, "StandardUTF8Type");
			keyspace.TryCreateColumnFamily<AsciiType, AsciiType>(server, "Super");

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
