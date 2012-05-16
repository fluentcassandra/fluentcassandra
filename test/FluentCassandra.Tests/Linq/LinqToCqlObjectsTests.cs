using System.Linq;
using FluentCassandra.Connections;
using Xunit;
using FluentCassandra.Types;
using System;

namespace FluentCassandra.Linq
{
	
	public class LinqToCqlObjectsTests : IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;

		public class User
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public int Age { get; set; }
		}

		public LinqToCqlObjectsTests()
		{
			var keyspaceName = "Testing";
			var server = new Server("localhost");

			_db = new CassandraContext(keyspace: keyspaceName, server: server);
			_family = _db.GetColumnFamily<AsciiType>("Users");
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private string ScrubLineBreaks(string query)
		{
			return query.Replace("\n", "");
		}

		private void AreEqual(string expected, string actual)
		{
			Assert.Equal(ScrubLineBreaks(expected), ScrubLineBreaks(actual));
		}

		[Fact]
		public void Provider()
		{
			var expected = "SELECT * FROM Users";

			var query = _family.AsObjectQueryable<User>().ToQuery();
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT()
		{
			var expected = "SELECT * FROM Users";

			var query =
				from f in _family.AsObjectQueryable<User>()
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void LIMIT()
		{
			var expected = "SELECT * FROM Users LIMIT 25";

			var query = (
				from f in _family.AsObjectQueryable<User>()
				select f).Take(25);

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT_One_Column()
		{
			var expected = "SELECT Age FROM Users";

			var query = _family.AsObjectQueryable<User>().Select(x => x.Age);
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT_Two_Columns()
		{
			var expected = "SELECT Age, Name FROM Users";

			var query = _family.AsObjectQueryable<User>().Select(x => new { x.Age, x.Name });
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Using_KEY()
		{
			var expected = "SELECT * FROM Users WHERE KEY = 1234";

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 1234
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE KEY = 1234 AND Age = 10";

			var query =
				from f in _family.AsObjectQueryable<User>()
				where f.Id == 1234 && f.Age == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Two_OR_Parameter()
		{
			Assert.Throws(typeof(NotSupportedException), delegate {
				var query =
					from f in _family.AsObjectQueryable<User>()
					where f.Id == 1234 || f.Age == 10
					select f;

				query.ToString();
			});
		}
	}
}