using System;
using System.Collections.Generic;
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
            var results = _db.ExecuteQuery("SELECT * FROM Cql3List").ToList();

            //assert
            Assert.Equal(1, results.Count());
            Assert.Equal(2, results.First().Columns.Count);

            var row = (FluentCqlRow)results.First();
            var id = row.GetColumn("id").ColumnValue.GetValue<int>();
            var taglist = row.GetColumn("taglist").ColumnValue.GetValue<List<string>>();

            Assert.Equal(1, id);
            Assert.Equal(2, taglist.Count);
            Assert.Equal("item1", taglist[0]);
            Assert.Equal("item2", taglist[1]);
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
