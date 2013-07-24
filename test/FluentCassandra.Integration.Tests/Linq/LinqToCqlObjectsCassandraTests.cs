using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Integration.Tests.Linq
{
	
	public class LinqToCqlObjectsCassandraTests : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily _family;
		private CassandraDatabaseSetup.User[] _users;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup(cqlVersion: CqlVersion.Cql);
			_db = setup.DB;
			_family = setup.UserFamily;
			_users = setup.Users;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		public class User
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public int Age { get; set; }
		}

		[Fact]
		public void SELECT()
		{
			var query =
				from f in _family.AsObjectQueryable<User>()
				select f;
			var actual = query.ToList().OrderBy(x => x.Id).ToList();

			Assert.Equal(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.Equal(objUser.Id, dbUser.Id);
				Assert.Equal(objUser.Name, dbUser.Name);
				Assert.Equal(objUser.Email, dbUser.Email);
				Assert.Equal(objUser.Age, dbUser.Age);
			}
		}

		[Fact]
		public void SELECT_One_Column()
		{
			var query = _family.AsObjectQueryable<User>().Select(x => x.Age);
			var actual = query.ToList().OrderBy(x => x).ToList();
			var users = _users.OrderBy(x => x.Age).ToList();

			Assert.Equal(users.Count, actual.Count);
			for (int i = 0; i < users.Count; i++)
			{
				var objUser = users[i];
				var dbAge = actual[i];

				Assert.Equal(objUser.Age, dbAge);
			}
		}

		[Fact]
		public void SELECT_Two_Columns()
		{
			var query = _family.AsObjectQueryable<User>().Select(x => new { x.Age, x.Name });
			var actual = query.ToList().OrderBy(x => x.Age).ToList();
			var users = _users.OrderBy(x => x.Age).ToList();

			Assert.Equal(users.Count, actual.Count);
			for (int i = 0; i < users.Count; i++)
			{
				var objUser = users[i];
				var dbUser = actual[i];

				Assert.Equal(objUser.Age, dbUser.Age);
				Assert.Equal(objUser.Name, dbUser.Name);
			}
		}

		[Fact]
		public void WHERE_Using_KEY()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 2
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, actual.Id);
			Assert.Equal(expected.Name, actual.Name);
			Assert.Equal(expected.Email, actual.Email);
			Assert.Equal(expected.Age, actual.Age);
		}

		[Fact]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 2 && f.Age == 23
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, actual.Id);
			Assert.Equal(expected.Name, actual.Name);
			Assert.Equal(expected.Email, actual.Email);
			Assert.Equal(expected.Age, actual.Age);
		}

		[Fact]
		public void WHERE_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Age == 23
				select f;
			var response = query.ToList();
			var actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, actual.Id);
			Assert.Equal(expected.Name, actual.Name);
			Assert.Equal(expected.Email, actual.Email);
			Assert.Equal(expected.Age, actual.Age);
		}
	}
}
