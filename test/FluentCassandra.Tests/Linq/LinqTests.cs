using System.Linq;
using FluentCassandra.Connections;
using NUnit.Framework;
using FluentCassandra.Types;
using System;

namespace FluentCassandra.Linq
{
	[TestFixture]
	public class LinqTests
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;

		[SetUp]
		public void TestInit()
		{
			var keyspaceName = "Testing";
			var server = new Server("localhost");

			_db = new CassandraContext(keyspace: keyspaceName, server: server);
			_family = _db.GetColumnFamily<AsciiType>("Users");
		}

		private string ScrubLineBreaks(string query)
		{
			return query.Replace("\n", "");
		}

		private void AreEqual(string expected, string actual)
		{
			Assert.AreEqual(ScrubLineBreaks(expected), ScrubLineBreaks(actual));
		}

		[Test]
		public void Provider()
		{
			var expected = "SELECT * FROM Users";

			var query = _family.ToQuery();
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT()
		{
			var expected = "SELECT * FROM Users";

			var query =
				from f in _family
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT_One_Column()
		{
			var expected = "SELECT Age FROM Users";

			var query = _family.Select("Age");
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT_Two_Columns()
		{
			var expected = "SELECT Age, Name FROM Users";

			var query = _family.Select("Age", "Name");
			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
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

		[Test]
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

		[Test]
		public void WHERE_Using_KEY_And_One_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE (KEY = 1234 AND Age = 10)";

			var query =
				from f in _family
				where f.Key == 1234 && f["Age"] == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
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

		[Test]
		public void WHERE_Two_AND_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE (Id = 1234 AND Age = 10)";

			var query =
				from f in _family
				where f["Id"] == 1234 && f["Age"] == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_Two_OR_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE (Id = 1234 OR Age = 10)";

			var query =
				from f in _family
				where f["Id"] == 1234 || f["Age"] == 10
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_Three_Complex_Parameter()
		{
			var expected = "SELECT * FROM Users WHERE ((Id = 1234 OR Age = 10) AND Name = 'Adama')";

			var query =
				from f in _family
				where (f["Id"] == 1234 || f["Age"] == 10) && f["Name"] == "Adama"
				select f;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT_Two_Columns_WHERE_Three_Complex_Parameter()
		{
			var expected = "SELECT Age, Name FROM Users WHERE ((Id = 1234 OR Age = 10) AND Name = 'Adama')";

			var query = _family
				.Where(f => (f["Id"] == 1234 || f["Age"] == 10) && f["Name"] == "Adama")
				.Select("Age", "Name");

			var actual = query.ToString();

			AreEqual(expected, actual);
		}
	}
}