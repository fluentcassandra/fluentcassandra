using System;
using System.Linq;
using FluentCassandra.Connections;
using NUnit.Framework;

namespace FluentCassandra.Operations
{
	[TestFixture]
	public class CqlTest
	{
		private CassandraContext _db;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
		}

		[TestFixtureTearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
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
				Assert.AreEqual(6, actual.Count());
			}
		}
	}
}
