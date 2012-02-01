using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraDatabaseSetup
	{
		public CassandraContext DB;
		public CassandraColumnFamily<AsciiType> Family;
		public CassandraSuperColumnFamily<AsciiType, AsciiType> SuperFamily;
		public CassandraColumnFamily<UTF8Type> UserFamily;

		public User[] Users;

		public class User
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public int Age { get; set; }
		}

		public const string TestKey1 = "Test1";
		public const string TestKey2 = "Test2";

		public const string TestStandardName = "Test1";
		public const string TestSuperName = "SubTest1";

		public CassandraDatabaseSetup(bool volitile = false)
		{
			var keyspaceName = "Testing";
			var server = new Server("localhost");

			DB = new CassandraContext(keyspaceName, server);
			DB.ThrowErrors = true;

			var exists = DB.KeyspaceExists(keyspaceName);

			Family = DB.GetColumnFamily<AsciiType>("Standard");
			SuperFamily = DB.GetColumnFamily<AsciiType, AsciiType>("Super");
			UserFamily = DB.GetColumnFamily<UTF8Type>("Users");
			Users = new[] {
				new User { Id = 1, Name = "Darren Gemmell", Email = "darren@somewhere.com", Age = 32 },
				new User { Id = 2, Name = "Fernando Laubscher", Email = "fernando@somewhere.com", Age = 23 },
				new User { Id = 3, Name = "Cody Millhouse", Email = "cody@somewhere.com", Age = 56 },
				new User { Id = 4, Name = "Emilia Thibert", Email = "emilia@somewhere.com", Age = 67 },
				new User { Id = 5, Name = "Allyson Schurr", Email = "allyson@somewhere.com", Age = 21 }
			};

			if (exists && !volitile)
				return;

			DB.DropKeyspace(keyspaceName);

			var keyspace = DB.Keyspace;
			keyspace.TryCreateSelf();
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

			DB.ExecuteNonQuery(@"
CREATE COLUMNFAMILY Users (
	KEY int PRIMARY KEY,
	Name ascii,
	Email ascii,
	Age int
);");
			DB.ExecuteNonQuery(@"CREATE INDEX User_Age ON Users (Age);");

			foreach (var user in Users)
			{
				dynamic record = UserFamily.CreateRecord(user.Id);
				record.Name = user.Name;
				record.Email = user.Email;
				record.Age = user.Age;

				DB.Attach(record);
			}
			DB.SaveChanges();
		}
	}
}
