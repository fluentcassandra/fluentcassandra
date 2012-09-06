using System.Linq;
using FluentCassandra.Connections;
using Xunit;
using FluentCassandra.Types;
using System;
using System.Configuration;

namespace FluentCassandra.Linq
{
	
	public class LinqToCqlTests : IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;

		public LinqToCqlTests()
		{
			var keyspaceName = ConfigurationManager.AppSettings["TestKeySpace"];
            var server = new Server(ConfigurationManager.AppSettings["TestServer"]);

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

			var query = _family.ToQuery();
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT()
		{
			var expected = "SELECT * FROM Users";

			var query =
				from f in _family
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void LIMIT()
		{
			var expected = "SELECT * FROM Users LIMIT 25";

			var query = (
				from f in _family
				select f).Take(25);

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT_One_Column()
		{
			var expected = "SELECT Age FROM Users";

			var query = _family.Select("Age");
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void SELECT_Two_Columns()
		{
			var expected = "SELECT Age, Name FROM Users";

			var query = _family.Select("Age", "Name");
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void Cannot_Use_Columns_Property()
		{
			var query =
				from f in _family
				where f.Columns.Count > 0
				select f;

			Assert.Throws<NotSupportedException>(delegate {
				query.ToString();
			});
		}

		[Fact]
		public void WHERE_Using_KEY()
		{
			var expected = "SELECT * FROM Users WHERE KEY = 1234";

			var query =
				from f in _family
				where f.Key == 1234
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE KEY = 1234 AND Age = 10";

			var query =
				from f in _family
				where f.Key == 1234 && f["Age"] == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_One_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE Id = 1234";

			var query =
				from f in _family
				where f["Id"] == 1234
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Two_AND_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE Id = 1234 AND Age = 10";

			var query =
				from f in _family
				where f["Id"] == 1234 && f["Age"] == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void WHERE_Two_OR_Parameter()
		{
			Assert.Throws(typeof(NotSupportedException), delegate {
				var query =
					from f in _family
					where f["Id"] == 1234 || f["Age"] == 10
					select f;

				query.ToString();
			});
		}

		[Fact]
		public void ORDER_BY()
		{
			var expected = "SELECT * FROM Users ORDER BY Age ASC";

			var query =
				from f in _family
				orderby f["Age"]
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void ORDER_BY_Two_Fields()
		{
			var expected = "SELECT * FROM Users ORDER BY Age ASC, Id DESC";

			var query =
				from f in _family
				orderby f["Age"], f["Id"] descending
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void ORDER_BY_Two_Fields_2()
		{
			var expected = "SELECT * FROM Users ORDER BY Age DESC, Id ASC";

			var query =
				from f in _family
				orderby f["Age"] descending, f["Id"]
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void ORDER_BY_ASC()
		{
			var expected = "SELECT * FROM Users ORDER BY Age ASC";

			var query =
				from f in _family
				orderby f["Age"] ascending
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void ORDER_BY_DESC()
		{
			var expected = "SELECT * FROM Users ORDER BY Age DESC";

			var query =
				from f in _family
				orderby f["Age"] descending
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Fact]
		public void Changed_Where_Expressions_Should_Translate_To_AND()
		{
			var expected = "SELECT * FROM Users WHERE Id = 1234 AND Age = 10";

			IQueryable<ICqlRow> query = _family;
			query = query.Where(q => q["Id"] == 1234).Where(q => q["Age"] == 10);

			var actual = query.ToString();
			AreEqual(expected, actual);
		}

	}
}