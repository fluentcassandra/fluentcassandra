using System;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Linq
{
	[TestFixture]
	public class LinqToCqlObjectsCassandraTests
	{
		private CassandraContext _db;
		private CassandraColumnFamily _family;
		private CassandraDatabaseSetup.User[] _users;

		public class User
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public int Age { get; set; }
		}

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
				from f in _family.AsObjectQueryable<User>()
				select f;
			var actual = query.ToList().OrderBy(x => x.Id).ToList();

			Assert.AreEqual(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.AreEqual(objUser.Id, dbUser.Id);
				Assert.AreEqual(objUser.Name, dbUser.Name);
				Assert.AreEqual(objUser.Email, dbUser.Email);
				Assert.AreEqual(objUser.Age, dbUser.Age);
			}
		}

		[Test]
		public void SELECT_One_Column()
		{
			var query = _family.AsObjectQueryable<User>().Select(x => x.Age);
			var actual = query.ToList().OrderBy(x => x).ToList();
			var users = _users.OrderBy(x => x.Age).ToList();

			Assert.AreEqual(users.Count, actual.Count);
			for (int i = 0; i < users.Count; i++)
			{
				var objUser = users[i];
				var dbAge = actual[i];

				Assert.AreEqual(objUser.Age, dbAge);
			}
		}

		[Test]
		public void SELECT_Two_Columns()
		{
			var query = _family.AsObjectQueryable<User>().Select(x => new { x.Age, x.Name });
			var actual = query.ToList().OrderBy(x => x.Age).ToList();
			var users = _users.OrderBy(x => x.Age).ToList();

			Assert.AreEqual(users.Count, actual.Count);
			for (int i = 0; i < users.Count; i++)
			{
				var objUser = users[i];
				var dbUser = actual[i];

				Assert.AreEqual(objUser.Age, dbUser.Age);
				Assert.AreEqual(objUser.Name, dbUser.Name);
			}
		}

		[Test]
		public void WHERE_Using_KEY()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 2
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Id);
			Assert.AreEqual(expected.Name, actual.Name);
			Assert.AreEqual(expected.Email, actual.Email);
			Assert.AreEqual(expected.Age, actual.Age);
		}

		[Test]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 2 && f.Age == 23
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Id);
			Assert.AreEqual(expected.Name, actual.Name);
			Assert.AreEqual(expected.Email, actual.Email);
			Assert.AreEqual(expected.Age, actual.Age);
		}

		[Test]
		public void WHERE_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Age == 23
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.AreEqual(1, response.Count);
			Assert.AreEqual(expected.Id, actual.Id);
			Assert.AreEqual(expected.Name, actual.Name);
			Assert.AreEqual(expected.Email, actual.Email);
			Assert.AreEqual(expected.Age, actual.Age);
		}
	}
}
