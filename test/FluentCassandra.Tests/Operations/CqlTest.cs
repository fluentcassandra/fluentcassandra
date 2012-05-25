using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Operations
{
	public class CqlTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
		public void With_Compression()
		{
			// arrange
			var insertQuery = @"INSERT INTO Users (KEY, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";

			// act
			_db.ExecuteNonQuery(insertQuery);
			var actual = _db.ExecuteQuery("SELECT * FROM Users");

			// assert
			Assert.Equal(6, actual.Count());
		}
	}
}
