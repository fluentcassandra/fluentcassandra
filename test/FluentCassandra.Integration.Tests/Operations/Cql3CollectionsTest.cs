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
        public void TestReadingCql3Set()
        {
            //arrange
            var guid1 = new Guid("88F1F2FE-B13B-4241-B17E-B8FAB8AC588B");
            var guid2 = new Guid("1AFBBD02-C4D5-46BD-B5F5-D0DCA91BC049");
            var guids = new[] {guid1, guid2};
            var insertQuery = @"INSERT INTO Cql3Set (Id, TagSet) VALUES(1, {"+ guid1 +","+ guid2 +"});";

            //act
            _db.ExecuteNonQuery(insertQuery);
            var results = _db.ExecuteQuery("SELECT * FROM Cql3Set").ToList();

            //assert
            Assert.Equal(1, results.Count());
            Assert.Equal(2, results.First().Columns.Count);

            var row = (FluentCqlRow)results.First();
            var id = row.GetColumn("id").ColumnValue.GetValue<int>();
            var tagset = row.GetColumn("tagset").ColumnValue.GetValue<List<Guid>>();

            Assert.Equal(1, id);
            Assert.Equal(2, tagset.Count);
            Assert.True(guids.Contains(tagset[0]));
            Assert.True(guids.Contains(tagset[1]));
        }

        [Fact]
        public void TestReadingCql3Map()
        {
            //arrange

            //act

            //assert
        }
    }
}
