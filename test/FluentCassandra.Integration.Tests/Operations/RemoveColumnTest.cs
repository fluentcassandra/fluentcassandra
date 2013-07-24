using System;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.Operations
{
	
	public class RemoveColumnTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

			setup.ResetFamily();
			setup.ResetSuperFamily();
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private const string _testKey = "Test1";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

		[Fact]
		public void Standard_RemoveColumn()
		{
			// arrange
			int expectedCount = 2;

			// act
			_family.RemoveColumn(_testKey, _testName);

			// assert
			int actualCount = _family.ColumnCount(_testKey, null, null);
			Assert.Equal(expectedCount, actualCount);
		}

		[Fact]
		public void Standard_RemoveKey()
		{
			// arrange
			int expectedCount = 0;

			// act
			_family.RemoveKey(_testKey);

			// assert
			int actualCount = _family.ColumnCount(_testKey, null, null);
			Assert.Equal(expectedCount, actualCount);
		}

		[Fact]
		public void Super_RemoveColumn()
		{
			// arrange
			int expectedCount = 2;

			// act
			_superFamily.RemoveColumn(_testKey, _testSuperName, _testName);

			// assert
			int actualCount = _superFamily.SuperColumnCount(_testKey, _testSuperName, null, null);
			Assert.Equal(expectedCount, actualCount);
		}

		[Fact]
		public void Super_RemoveSuperColumn()
		{
			// arrange
			int expectedCount = 0;

			// act
			_superFamily.RemoveColumn(_testKey, _testSuperName);

			// assert
			int actualCount = _superFamily.ColumnCount(_testKey, null, null);
			Assert.Equal(expectedCount, actualCount);
		}

		[Fact]
		public void Super_RemoveKey()
		{
			// arrange
			int expectedCount = 0;

			// act
			_superFamily.RemoveKey(_testKey);

			// assert
			int actualCount = _superFamily.ColumnCount(_testKey, null, null);
			Assert.Equal(expectedCount, actualCount);
		}
	}
}
