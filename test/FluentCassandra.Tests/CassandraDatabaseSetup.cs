﻿using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using System.Collections.Generic;
using System.Configuration;

namespace FluentCassandra
{
	public class CassandraDatabaseSetup
	{
		public ConnectionBuilder ConnectionBuilder;
		public CassandraContext DB;

		public CassandraColumnFamily<AsciiType> Family;
		public CassandraSuperColumnFamily<AsciiType, AsciiType> SuperFamily;
		public CassandraColumnFamily UserFamily;

		public User[] Users = new[] {
					new User { Id = 1, Name = "Darren Gemmell", Email = "darren@somewhere.com", Age = 32 },
					new User { Id = 2, Name = "Fernando Laubscher", Email = "fernando@somewhere.com", Age = 23 },
					new User { Id = 3, Name = "Cody Millhouse", Email = "cody@somewhere.com", Age = 56 },
					new User { Id = 4, Name = "Emilia Thibert", Email = "emilia@somewhere.com", Age = 67 },
					new User { Id = 5, Name = "Allyson Schurr", Email = "allyson@somewhere.com", Age = 21 }
				};

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

		public static readonly string Keyspace = ConfigurationManager.AppSettings["TestKeySpace"];
        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);
		
		public CassandraDatabaseSetup(bool reset = false)
		{
			ConnectionBuilder = new ConnectionBuilder(keyspace: Keyspace, server: Server);
			DB = new CassandraContext(ConnectionBuilder);
			
			var exists = DB.KeyspaceExists(Keyspace);

			Family = DB.GetColumnFamily<AsciiType>("Standard");
			SuperFamily = DB.GetColumnFamily<AsciiType, AsciiType>("Super");
			UserFamily = DB.GetColumnFamily("Users");

			if (exists && !reset)
				return;

			ResetDatabase();
		}

		public void ResetDatabase()
		{
			using (var session = new CassandraSession(ConnectionBuilder))
			using (var db = new CassandraContext(session))
			{
				db.ThrowErrors = true;
				db.TryDropKeyspace(Keyspace);

				var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema {
					Name = Keyspace
				}, db);
				db.Keyspace = keyspace;

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
				keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
					FamilyName = "StandardCompositeType",
					ColumnNameType = CassandraType.CompositeType(new[] { CassandraType.AsciiType, CassandraType.DoubleType })
				});
				keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
					FamilyName = "StandardDynamicCompositeType",
					ColumnNameType = CassandraType.DynamicCompositeType(new Dictionary<char, CassandraType> { { 'a', CassandraType.AsciiType }, { 'd', CassandraType.DoubleType } })
				});

				db.ExecuteNonQuery(@"
CREATE COLUMNFAMILY Users (
	Id int PRIMARY KEY,
	Name ascii,
	Email ascii,
	Age int
);");
				db.ExecuteNonQuery(@"CREATE INDEX User_Age ON Users (Age);");
				db.Keyspace.ClearCachedKeyspaceSchema();

				var family = db.GetColumnFamily<AsciiType>("Standard");
				var superFamily = db.GetColumnFamily<AsciiType, AsciiType>("Super");
				var userFamily = db.GetColumnFamily("Users");

				ResetFamily(family);
				ResetSuperFamily(superFamily);
				ResetUsersFamily(userFamily);
			}
		}

		public void ResetUsersFamily(CassandraColumnFamily userFamily = null)
		{
			userFamily = userFamily ?? UserFamily;

			var db = userFamily.Context;
			userFamily.RemoveAllRows();

			foreach (var user in Users)
			{
				dynamic record = userFamily.CreateRecord(user.Id);
				record.Name = user.Name;
				record.Email = user.Email;
				record.Age = user.Age;

				db.Attach(record);
			}

			db.SaveChanges();
		}

		public void ResetFamily(CassandraColumnFamily family = null)
		{
			family = family ?? Family;

			family.RemoveAllRows();

			family.InsertColumn(TestKey1, "Test1", Math.PI);
			family.InsertColumn(TestKey1, "Test2", Math.PI);
			family.InsertColumn(TestKey1, "Test3", Math.PI);

			family.InsertColumn(TestKey2, "Test1", Math.PI);
			family.InsertColumn(TestKey2, "Test2", Math.PI);
			family.InsertColumn(TestKey2, "Test3", Math.PI);
		}

		public void ResetSuperFamily(CassandraSuperColumnFamily superFamily = null)
		{
			superFamily = superFamily ?? SuperFamily;

			superFamily.RemoveAllRows();

			superFamily.InsertColumn(TestKey1, TestSuperName, "Test1", Math.PI);
			superFamily.InsertColumn(TestKey1, TestSuperName, "Test2", Math.PI);
			superFamily.InsertColumn(TestKey1, TestSuperName, "Test3", Math.PI);

			superFamily.InsertColumn(TestKey2, TestSuperName, "Test1", Math.PI);
			superFamily.InsertColumn(TestKey2, TestSuperName, "Test2", Math.PI);
			superFamily.InsertColumn(TestKey2, TestSuperName, "Test3", Math.PI);
		}
	}
}
