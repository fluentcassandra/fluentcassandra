using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	
	public class GetColumnTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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
		public void Standard_GetColumn()
		{
			// arrange
			double expected = Math.PI;

			// act
			var column = _family.GetColumn(_testKey, _testName);

			// assert
			Assert.Equal(_testName, (string)column.ColumnName);
			Assert.Equal(expected, (double)column.ColumnValue);
		}

		[Fact]
		public void Super_GetColumn()
		{
			// arrange
			double expected = Math.PI;

			// act
			var column = _superFamily.GetColumn(_testKey, _testSuperName, _testName);

			// assert
			Assert.Equal(_testName, (string)column.ColumnName);
			Assert.Equal(expected, (double)column.ColumnValue);
		}

		[Fact]
		public void Super_GetSuperColumn()
		{
			// arrange

			// act
			var column = _superFamily.GetSuperColumn(_testKey, _testSuperName);

			// assert
			Assert.Equal(_testSuperName, (string)column.ColumnName);
		}
	}
}
