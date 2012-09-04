using System;
using System.Linq;
using FluentCassandra.Connections;
using Xunit;

namespace FluentCassandra.Operations
{
    /// <summary>
    /// The purpose of this test is to check compatibility beetween CQL2 and CQL3 support in Cassandra
    /// We fouund out that case-insensitivity in CQL3 column names generates problems when used together with CQL2 column family supoort
    /// </summary>
    public class Cql3vs2CompatibilityTest : IUseFixture<CompatibilityCassandraDatabaseSetupFixture>, IDisposable
    {
        private CassandraContext _db;
        CompatibilityCassandraDatabaseSetupFixture _data;

        public void SetFixture(CompatibilityCassandraDatabaseSetupFixture data)
        {
            _data = data;
        }


        public void Dispose()
        {
            _db.Dispose();
        }

        private void Common(CompatibilityCassandraDatabaseSetup setup)
        {
            _db = setup.DB;

            // arrange
            var insertQuery = @"INSERT INTO users (Id, Name, Email, Age) VALUES (23, '" + new String('X', 200) + "', 'test@test.com', 43)";

            // act
            _db.ExecuteNonQuery(insertQuery);
            var actual = _db.ExecuteQuery("SELECT * FROM users");

            // assert
            Assert.Equal(6, actual.Count());
        }


        /// <summary>
        /// Bug 1 - both are case sensitive (column names & dynamic fields)
        /// </summary>
        [Fact]
        public void IgnoreCase1()
        {
            var setup = _data.DatabaseSetup(null,false,false);
            Common(setup);
        }
        /// <summary>
        /// Bug 2 - column names are case sensitive
        /// </summary>
        [Fact]
        public void IgnoreCase2()
        {
            var setup = _data.DatabaseSetup(null,true,false);
            Common(setup);
        }

        /// <summary>
        /// Bug 3 - dynamic fields names are case sensitive
        /// </summary>
        [Fact]
        public void IgnoreCase3()
        {
            var setup = _data.DatabaseSetup(null,false, true);
            Common(setup);
        }

        /// <summary>
        /// Only this one is ok column names are case-sensitives & dynamic fields are lover cased
        /// </summary>
        [Fact]
        public void IgnoreCase4()
        {
            var setup = _data.DatabaseSetup(null, true, true);
            Common(setup);
        }
    }
}
