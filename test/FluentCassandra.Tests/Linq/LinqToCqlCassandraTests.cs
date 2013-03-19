using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Linq
{	
	public class LinqToCqlCassandraTests : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		[Fact]
		public void SELECT()
		{
			var query =
				from f in _family
				select f;
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.Equal(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				dynamic dbUser = actual[i];

				Assert.Equal(objUser.Id, (int)dbUser.Key);
				Assert.Equal(objUser.Name, (string)dbUser.Name);
				Assert.Equal(objUser.Email, (string)dbUser.Email);
				Assert.Equal(objUser.Age, (int)dbUser.Age);
			}
		}

		[Fact]
		public void SELECT_One_Column()
		{
			var query = _family.Select("Age");
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.Equal(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.Equal(1, dbUser.Columns.Count);
				Assert.Equal(objUser.Age, (int)dbUser["Age"]);
			}
		}

		[Fact]
		public void SELECT_Two_Columns()
		{
			var query = _family.Select("Age", "Name");
			var actual = query.ToList().OrderBy(x => (int)x.Key).ToList();

			Assert.Equal(_users.Length, actual.Count);
			for (int i = 0; i < _users.Length; i++)
			{
				var objUser = _users[i];
				var dbUser = actual[i];

				Assert.Equal(2, dbUser.Columns.Count);
				Assert.Equal(objUser.Age, (int)dbUser["Age"]);
				Assert.Equal(objUser.Name, (string)dbUser["Name"]);
			}
		}

		[Fact]
		public void WHERE_Using_KEY()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f.Key == 2
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, (int)actual.Key);
			Assert.Equal(expected.Name, (string)actual.Name);
			Assert.Equal(expected.Email, (string)actual.Email);
			Assert.Equal(expected.Age, (int)actual.Age);
		}

		[Fact]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f.Key == 2 && f["Age"] == 23
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, (int)actual.Key);
			Assert.Equal(expected.Name, (string)actual.Name);
			Assert.Equal(expected.Email, (string)actual.Email);
			Assert.Equal(expected.Age, (int)actual.Age);
		}

		[Fact]
		public void WHERE_One_Parameter()
		{
			var expected = _users.Where(x => x.Id == 2).FirstOrDefault();

			var query =
				from f in _family
				where f["Age"] == 23
				select f;
			var response = query.ToList();
			dynamic actual = response.FirstOrDefault();

			Assert.Equal(1, response.Count);
			Assert.Equal(expected.Id, (int)actual.Key);
			Assert.Equal(expected.Name, (string)actual.Name);
			Assert.Equal(expected.Email, (string)actual.Email);
			Assert.Equal(expected.Age, (int)actual.Age);
		}
	}
}
