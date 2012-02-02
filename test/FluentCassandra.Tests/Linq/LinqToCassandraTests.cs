using System;
using System.Linq;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra.Linq
{
	[TestFixture]
	public class LinqToCassandraTests
	{
		private CassandraContext _db;
		private CassandraColumnFamily<UTF8Type> _family;
		private CassandraDatabaseSetup.User[] _users;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
			_family = setup.UserFamily;
			_users = setup.Users;
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

				Assert.AreEqual(objUser.Id, dbUser.KeyType);
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
			Assert.AreEqual(expected.Id, actual.KeyType);
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
			Assert.AreEqual(expected.Id, actual.KeyType);
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
			Assert.AreEqual(expected.Id, actual.KeyType);
			Assert.AreEqual(expected.Name, (string)actual.Name);
			Assert.AreEqual(expected.Email, (string)actual.Email);
			Assert.AreEqual(expected.Age, (int)actual.Age);
		}
	}
}
