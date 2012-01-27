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
			keyspace.TryCreateColumnFamily<AsciiType>(server, "Users");

			_db = new CassandraContext(keyspace: keyspaceName, server: server);
			_db.ThrowErrors = true;
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
			// arrange

			// act
			var query =
				from f in _family
				select f;
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			// assert
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
	}
}
