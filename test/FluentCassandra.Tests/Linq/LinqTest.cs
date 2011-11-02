using System.Linq;
using NUnit.Framework;
using FluentCassandra.Linq;

namespace FluentCassandra.Tests.Linq
{
	[TestFixture]
	public class LinqTests
	{
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
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users";

			var query = new CqlQueryProvider(session, "Users").ToQuery();

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void SELECT_One_Column()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age FROM Users";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				select new { user.Age };

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_One_Parameter()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users WHERE Id = @param0";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where user.Id == 1234
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_Two_AND_Parameter()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users WHERE (Id = @param0 AND Age = @param1)";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where user.Id == 1234 && user.Age == 10
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_Two_OR_Parameter()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users WHERE (Id = @param0 OR Age = @param1)";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where user.Id == 1234 || user.Age == 10
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_Three_Complex_Parameter()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users WHERE ((Id = @param0 OR Age = @param1) AND Name = @param2)";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where (user.Id == 1234 || user.Age == 10) && user.Name == "Adama"
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void ORDERBY()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users ORDER BY Age ASC";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				orderby user.Age
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void ORDERBY_Two_Parameters()
		{
			var session = new CassandraSession();
			var expected = "SELECT * FROM Users ORDER BY Age ASC, Name ASC";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				orderby user.Age, user.Name
				select user;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void GROUPBY()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, COUNT(*) FROM Users GROUP BY Age";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by user.Age;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void GROUPBY_Two_Parameters()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, Name, COUNT(*) FROM Users GROUP BY Age, Name";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by new { user.Age, user.Name };

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void GROUPBY_Custom_SELECT()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age FROM Users GROUP BY Age";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by user.Age into userGroup
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void GROUPBY_Two_Parameters_Custom_SELECT()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, Name FROM Users GROUP BY Age, Name";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by new { user.Age, user.Name } into userGroup
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void HAVING()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age FROM Users GROUP BY Age HAVING Age = @param0";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by new { user.Age } into userGroup
				where userGroup.Key.Age == 10
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void HAVING_Two_Parameters()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, Name FROM Users GROUP BY Age, Name HAVING (Age = @param0 AND Name = @param1)";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				group user by new { user.Age, user.Name } into userGroup
				where userGroup.Key.Age == 10 && userGroup.Key.Name == "Adama"
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_and_HAVING()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, Name FROM Users WHERE Age = @param0 GROUP BY Age, Name HAVING Name = @param1";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where user.Age == 10
				group user by new { user.Age, user.Name } into userGroup
				where userGroup.Key.Name == "Adama"
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}

		[Test]
		public void WHERE_and_HAVING_and_ORDERBY()
		{
			var session = new CassandraSession();
			var expected = "SELECT Age, Name FROM Users WHERE Age = @param0 GROUP BY Age, Name HAVING Name = @param1 ORDER BY Age ASC";

			var query =
				from user in new CqlQueryProvider(session, "Users")
				where user.Age == 10
				group user by new { user.Age, user.Name } into userGroup
				where userGroup.Key.Name == "Adama"
				orderby userGroup.Key.Age
				select userGroup.Key;

			var actual = query.ToString();

			AreEqual(expected, actual);
		}
	}
}