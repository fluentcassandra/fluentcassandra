using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Integration.Tests.Operations
{
    /// <summary>
    /// fluent-cassandra support for CQL3 collection types, such as map / list / set
    /// </summary>
    public class Cql3CollectionsTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
    {
        private CassandraContext _db;

        public void SetFixture(CassandraDatabaseSetupFixture data)
        {
            var setup = data.DatabaseSetup(cqlVersion: CqlVersion.Cql3);
            _db = setup.DB;
        }

        public void Dispose()
        {
            if(_db != null)
                _db.Dispose();
        }

        [Fact]
        public void TestReadingCql3List()
        {
            //arrange
            var insertQuery = @"INSERT INTO Cql3List (Id, TagList) VALUES(1, ['item1','item2']);";

            //act
            _db.ExecuteNonQuery(insertQuery);
            var results = _db.ExecuteQuery("SELECT * FROM Cql3List");

            //assert
            Assert.Equal(1, results.Count());
        }

        [Fact]
        public void TestReadingCql3Map()
        {
            //arrange

            //act

            //assert
        }

        [Fact]
        public void TestReadingCql3Set()
        {
            //arrange

            //act

            //assert
        }
    }
}
