using System;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.Operations
{
	
	public class InsertColumnTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private const string _testKey = "Test1";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

		[Fact]
		public void ColumnFamily()
		{
			// arrange
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;
			int timeToLive = 1;

			// act
			_family.InsertColumn(_testKey, _testName, value, timestamp, timeToLive);
			var column = _family.Get(_testKey).Execute();
			var actual = column.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.Equal(_testName, (string)actual.ColumnName);
			Assert.Equal(value, (double)actual.ColumnValue);
		}

		[Fact]
		public void SuperColumnFamily()
		{
			// arrange
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;
			int timeToLive = 1;

			// act
			_superFamily.InsertColumn(_testKey, _testSuperName, _testName, value, timestamp, timeToLive);
			var column = _family.Get(_testKey).Execute();
			var actual = column.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.Equal(_testName, (string)actual.ColumnName);
			Assert.Equal(value, (double)actual.ColumnValue);
		}
	}
}