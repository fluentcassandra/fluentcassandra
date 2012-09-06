using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Operations
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
            var setup = data.DatabaseSetup();
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
            var insertQuery = @"INSERT INTO users (Id, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";
            var insertQuery2 = @"INSERT INTO users (Id, Name, Email, Age) VALUES (23, '" + new String('Y', 200) + "', 'test@test.com', 53)";

            // act
            _db.ExecuteNonQuery(insertQuery);
            _db.ExecuteNonQuery(insertQuery2);
            var actual = _db.ExecuteQuery("SELECT * FROM users");

            // assert
            Assert.Equal(6, actual.Count());
        }

        [Fact]
        public void TestLinq()
        {
            // arrange
            var insertQuery = @"INSERT INTO users (Id, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";

            // act
            _db.ExecuteNonQuery(insertQuery);

            var table = _db.GetColumnFamily("users");

            var q = from row in table select row;

            var actual = q.ToList();

            // assert
            Assert.Equal(6, actual.Count());
        }

        /// <summary>
        /// Count() is not woriking 
        /// </summary>
        [Fact]
        public void TestLinq_CountDoNotWork()
        {
            // arrange
            var insertQuery = @"INSERT INTO users (Id, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";

            // act
            _db.ExecuteNonQuery(insertQuery);

            var table = _db.GetColumnFamily("users");

            var q = from row in table select row;

            var actualCount = q.Count();

            // assert
            Assert.Equal(6, actualCount);
        }
    }
}
