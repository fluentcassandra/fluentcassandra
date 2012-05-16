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
			var cb = new ConnectionBuilder(
				keyspace: _db.ConnectionBuilder.Keyspace,
				host: _db.ConnectionBuilder.Servers[0].Host,
				port: _db.ConnectionBuilder.Servers[0].Port,
				connectionTimeout: _db.ConnectionBuilder.Servers[0].Timeout,
				compressCqlQueries: true
			);

			// arrange
			var insertQuery = @"INSERT INTO Users (KEY, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";

			using (var db = new CassandraContext(cb))
			{
				// act
				db.ExecuteNonQuery(insertQuery);
				var actual = db.ExecuteQuery("SELECT * FROM Users");

				// assert
				Assert.Equal(6, actual.Count());
			}
		}
	}
}
