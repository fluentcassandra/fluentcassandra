using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Integration.Tests.Operations
{
	/// <summary>
	/// Basic fluent-cassandra support for CQL3
	/// Composite keys/Count
	/// </summary>
	public class Cql3Test : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup(cqlVersion: CqlVersion.Cql3);
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
		public void TestOverwritingOfUsersOnPrimaryKeys()
		{
			// arrange
			var insertQuery = @"INSERT INTO ""Users"" (""Id"", ""Name"", ""Email"", ""Age"") VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";
			var insertQuery2 = @"INSERT INTO ""Users"" (""Id"", ""Name"", ""Email"", ""Age"") VALUES (23, '" + new String('Y', 200) + "', 'test@test.com', 53)";

			// act
			_db.ExecuteNonQuery(insertQuery);
			_db.ExecuteNonQuery(insertQuery2);
			var actual = _db.ExecuteQuery("SELECT * FROM \"Users\"");

			// assert
			Assert.Equal(6, actual.Count());
		}

		[Fact]
		public void TestLinq()
		{
			// arrange
			var insertQuery = @"INSERT INTO ""Users"" (""Id"", ""Name"", ""Email"", ""Age"") VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";
			_db.ExecuteNonQuery(insertQuery);

			// act
			var table = _db.GetColumnFamily("Users");
			var q = from row in table select row;
			var actual = q.ToList();

			// assert
			Assert.Equal(6, actual.Count());
		}

		/// <summary>
		/// Count() is not working 
		/// </summary>
		[Fact]
		public void TestLinq_CountDoNotWork()
		{
			// arrange
			var insertQuery = @"INSERT INTO ""Users"" (""Id"", ""Name"", ""Email"", ""Age"") VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";
			_db.ExecuteNonQuery(insertQuery);

			// act
			var table = _db.GetColumnFamily("Users");
			var q = from row in table select row;
			var actualCount = q.Count();

			// assert
			Assert.Equal(6, actualCount);
		}
	}
}
