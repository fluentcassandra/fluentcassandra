using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Bugs
{
	public class Issue36KeyAliasSupport: IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup(false);
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
		public void Test_Cql2()
		{
			var connBuilder = _db.ConnectionBuilder;
			connBuilder = new ConnectionBuilder(connBuilder.Keyspace, connBuilder.Servers[0], cqlVersion: CqlVersion.Cql2);
			var db = new CassandraContext(connBuilder);

			// arrange
			db.TryExecuteNonQuery("DROP COLUMNFAMILY Users_Issue36");

			db.ExecuteNonQuery(@"
CREATE COLUMNFAMILY Users_Issue36 (
	UserName text PRIMARY KEY, 
	LastLogin timestamp);");

			db.ExecuteNonQuery(@"
BEGIN BATCH
	INSERT INTO Users_Issue36 (UserName, LastLogin) VALUES ('nberardi', '2012-6-6T04:30:00')
	INSERT INTO Users_Issue36 (UserName, LastLogin) VALUES ('jdoe', '2012-10-31T00:30:02')
	INSERT INTO Users_Issue36 (UserName, LastLogin) VALUES ('akim', '2003-6-6T05:35:23')
	INSERT INTO Users_Issue36 (UserName, LastLogin) VALUES ('jboes', '2001-1-1T13:02:10')
APPLY BATCH;
");

			// act
			var actual = db.ExecuteQuery("SELECT * FROM Users_Issue36");

			// assert
			var results = actual.ToList();
			Assert.Equal(4, results.Count);
		}

		[Fact]
		public void Test_Cql3()
		{
			var connBuilder = _db.ConnectionBuilder;
			connBuilder = new ConnectionBuilder(connBuilder.Keyspace, connBuilder.Servers[0], cqlVersion: CqlVersion.Cql3);
			var db = new CassandraContext(connBuilder);

			// arrange
			db.TryExecuteNonQuery("DROP TABLE Timeline_Issue36");

			db.ExecuteNonQuery(@"
CREATE TABLE Timeline_Issue36 (
	user_id varchar,
	tweet_id int,
	author text,
	body varchar,
	PRIMARY KEY (user_id, tweet_id));");

			db.ExecuteNonQuery(@"
BEGIN BATCH
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('nberardi', 1, 'nberardi', 'test 1234')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('nberardi', 2, 'nberardi', 'test 4567')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('nberardi', 3, 'nberardi', 'test 8910')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('jdoe', 1, 'nberardi', 'test 1111')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('akim', 1, 'nberardi', 'test 2222')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('jboes', 1, 'nberardi', 'test 3333')
	INSERT INTO Timeline_Issue36 (user_id, tweet_id, author, body) VALUES ('jboes', 2, 'nberardi', 'test 4444')
APPLY BATCH;
");

			// act
			var actual = db.ExecuteQuery("SELECT * FROM Timeline_Issue36");

			// assert
			var results = actual.ToList();
			Assert.Equal(7, results.Count);
		}
	}
}
