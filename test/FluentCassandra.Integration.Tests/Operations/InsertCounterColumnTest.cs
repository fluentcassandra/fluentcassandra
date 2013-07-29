using System;
using System.Linq;
using FluentCassandra.Types;
using Xunit;


namespace FluentCassandra.Integration.Tests.Operations
{
    public class InsertCounterColumnTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
    {
        private CassandraContext _db;
        private CassandraColumnFamily _counterFamily;
        private CassandraSuperColumnFamily _superCounterFamily;

        public InsertCounterColumnTest()
        {
            //Create a new row each time we run the test, so the expected counter values are always equal to 1
            _testKey = Guid.NewGuid().ToString(); 
        }

        public void SetFixture(CassandraDatabaseSetupFixture data)
        {
            var setup = data.DatabaseSetup();
            _db = setup.DB;
            _counterFamily = setup.CounterFamily;
            _superCounterFamily = setup.SuperCounterFamily;
        }

        public void Dispose()
        {
            _db.Dispose();
        }

        private readonly string _testKey;
        private const string _testName = "Test1";
        private const string _testSuperName = "SubTest1";

        [Fact]
        public void CounterColumnFamily()
        {
            //arrange
            long value = 1L;

            //act
            _counterFamily.InsertCounterColumn(_testKey, _testName, value);
            var column = _counterFamily.Get(_testKey).Execute();
            var actual = column.FirstOrDefault().Columns.FirstOrDefault();

            // assert
            Assert.Equal(_testName, (string)actual.ColumnName);
            Assert.Equal(value, (double)actual.ColumnValue);
        }

        [Fact]
        public void SuperCounterColumnFamily()
        {
            //arrange
            long value = 1L;

            //act
            _superCounterFamily.InsertCounterColumn(_testKey, _testSuperName, _testName, value);
            var column = _superCounterFamily.Get(_testKey).Execute();
            var actual = column.FirstOrDefault().Columns.FirstOrDefault();

            // assert
            Assert.Equal(_testSuperName, (string)actual.ColumnName);
            Assert.Equal(_testName, (string)actual.Columns[0].ColumnName);
            Assert.Equal(value, (double)actual.Columns[0].ColumnValue);
        }
    }
}
