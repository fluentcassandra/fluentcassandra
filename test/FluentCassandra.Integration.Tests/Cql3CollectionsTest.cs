using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Integration.Tests
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
        public void TestReadingCql3Set()
        {
            //arrange
            var insertQuery = @"INSERT INTO Cql3Collections (Id, TagSet) VALUES(1, {'item1','item2'});";

            //act
            _db.ExecuteNonQuery(insertQuery);
            var results = _db.ExecuteQuery("SELECT * FROM Cql3Collections");

            //assert
            Assert.Equal(4, results.Count());
        }

        [Fact]
        public void TestReadingCql3Map()
        {
            //arrange

            //act

            //assert
        }

        [Fact]
        public void TestReadingCql3List()
        {
            //arrange

            //act

            //assert
        }
    }
}
