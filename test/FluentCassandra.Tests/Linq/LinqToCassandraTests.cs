using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra.Linq
{
	[TestFixture]
	public class LinqToCassandraTests
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private User[] _users;

		[SetUp]
		public void TestInit()
		{
			var keyspaceName = "Testing";
			var server = new Server("localhost");

			var keyspace = new CassandraKeyspace(keyspaceName);
			keyspace.TryCreateSelf(server);

			_db = new CassandraContext(keyspace: keyspaceName, server: server);
			_db.ThrowErrors = true;

			_db.ExecuteNonQuery(@"DROP COLUMNFAMILY Users;");
			_db.ExecuteNonQuery(@"
CREATE COLUMNFAMILY Users (
	KEY int PRIMARY KEY,
	Name ascii,
	Email ascii,
	Age int
);");
			_db.ExecuteNonQuery(@"CREATE INDEX User_Age ON Users (Age);");

			_family = _db.GetColumnFamily<AsciiType>("Users");
			_family.RemoveAllRows();

			_users = new[] {
				new User { Id = 1, Name = "Darren Gemmell", Email = "darren@somewhere.com", Age = 32 },
				new User { Id = 2, Name = "Fernando Laubscher", Email = "fernando@somewhere.com", Age = 23 },
				new User { Id = 3, Name = "Cody Millhouse", Email = "cody@somewhere.com", Age = 56 },
				new User { Id = 4, Name = "Emilia Thibert", Email = "emilia@somewhere.com", Age = 67 },
				new User { Id = 5, Name = "Allyson Schurr", Email = "allyson@somewhere.com", Age = 21 }
			};

			foreach (var user in _users)
			{
				dynamic record = _family.CreateRecord(user.Id);
				record.Name = user.Name;
				record.Email = user.Email;
				record.Age = user.Age;

				_db.Attach(record);
			}
			_db.SaveChanges();
		}

		private class User
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public int Age { get; set; }
		}

		[Test]
		public void SELECT()
		{
			var query =
				from f in _family
				select f;
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.AreEqual(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				dynamic dbUser = actual[i];

				Assert.AreEqual(objUser.Id, dbUser.Key);
				Assert.AreEqual(objUser.Name, (string)dbUser.Name);
				Assert.AreEqual(objUser.Email, (string)dbUser.Email);
				Assert.AreEqual(objUser.Age, (int)dbUser.Age);
			}
		}

		[Test]
		public void SELECT_One_Column()
		{
			var query = _family.Select("Age");
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.AreEqual(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.AreEqual(1, dbUser.Columns.Count);
				Assert.AreEqual(objUser.Age, (int)dbUser["Age"]);
			}
		}

		[Test]
		public void SELECT_Two_Columns()
		{
			var query = _family.Select("Age", "Name");
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.AreEqual(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.AreEqual(2, dbUser.Columns.Count);
				Assert.AreEqual(objUser.Age, (int)dbUser["Age"]);
				Assert.AreEqual(objUser.Name, (string)dbUser["Name"]);
			}
		}

		[Test]
		public void WHERE_Using_KEY()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f.Key == 2
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Key);
			Assert.AreEqual(expected.Name, (string)actual.Name);
			Assert.AreEqual(expected.Email, (string)actual.Email);
			Assert.AreEqual(expected.Age, (int)actual.Age);
		}

		[Test]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f.Key == 2 && f["Age"] == 23
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Key);
			Assert.AreEqual(expected.Name, (string)actual.Name);
			Assert.AreEqual(expected.Email, (string)actual.Email);
			Assert.AreEqual(expected.Age, (int)actual.Age);
		}

		[Test]
		public void WHERE_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f["Age"] == 23
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Key);
			Assert.AreEqual(expected.Name, (string)actual.Name);
			Assert.AreEqual(expected.Email, (string)actual.Email);
			Assert.AreEqual(expected.Age, (int)actual.Age);
		}
	}
}
